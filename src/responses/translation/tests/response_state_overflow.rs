use super::*;

use axum::body::Bytes;
use futures_util::{Stream, StreamExt};
use std::convert::Infallible;
use std::pin::Pin;

type ResponseBody = Pin<Box<dyn Stream<Item = Result<Bytes, Infallible>> + Send>>;

fn validate_matrix_output(
    chunks: &[Bytes],
    series: &crate::ws_pump::SyntheticSeries,
    kind: crate::ws_pump::SyntheticSeriesKind,
    overflow: bool,
) -> serde_json::Value {
    assert!(chunks.len() <= series.events.len() + 4);
    assert_eq!(chunks.last().unwrap(), "data: [DONE]\n\n");
    let mut reconstructed = String::new();
    let mut correspondence = Vec::new();
    let mut completed = 0;
    let mut failed = 0;
    for (output, chunk) in chunks[..chunks.len() - 1].iter().enumerate() {
        let event = finite_sse_value(chunk);
        if event["type"] == "response.failed" {
            failed += 1;
            assert!(overflow);
            assert_eq!(
                event["response"]["error"]["code"],
                "upstream_inbound_buffer_overflow"
            );
        } else {
            assert_eq!(event["sequence_number"], correspondence.len());
            correspondence.push(
                json!({"output":output,"input":event["sequence_number"],"bytes":chunk.len()}),
            );
        }
        if let Some(fragment) = event["delta"].as_str() {
            reconstructed.push_str(fragment);
        }
        if event["type"] == "response.completed" {
            completed += 1;
            match kind {
                crate::ws_pump::SyntheticSeriesKind::Text => assert_eq!(
                    event["response"]["output"][0]["content"][0]["text"],
                    series.semantic_text
                ),
                crate::ws_pump::SyntheticSeriesKind::Arguments => assert_eq!(
                    serde_json::from_str::<serde_json::Value>(
                        event["response"]["output"][0]["arguments"]
                            .as_str()
                            .unwrap()
                    )
                    .unwrap()["s"],
                    series.semantic_text
                ),
            }
        }
    }
    assert_eq!((completed, failed), if overflow { (0, 1) } else { (1, 0) });
    if !overflow {
        assert_eq!(correspondence.len(), series.events.len());
        match kind {
            crate::ws_pump::SyntheticSeriesKind::Text => {
                assert_eq!(reconstructed, series.semantic_text)
            }
            crate::ws_pump::SyntheticSeriesKind::Arguments => assert_eq!(
                serde_json::from_str::<serde_json::Value>(&reconstructed).unwrap()["s"],
                series.semantic_text
            ),
        }
    }
    json!({"correspondence":correspondence,"delivered_inputs":correspondence.len(),"completed_chunks":completed,"failed_chunks":failed,"done_chunks":1,
        "output_chunk_bytes":chunks.iter().map(Bytes::len).collect::<Vec<_>>()})
}

async fn matrix_take(body: &mut ResponseBody, trace: &FiniteTrace, chunks: &mut Vec<Bytes>) {
    assert!(chunks.len() < 1547);
    chunks.push(
        trace
            .observe_operation(crate::ws_pump::TraceOperation::BodyNext, body.next())
            .await
            .unwrap()
            .unwrap(),
    );
}

async fn controlled_matrix_case(
    name: &str,
    workers: usize,
    capacity: usize,
    preload: usize,
    mode: (bool, bool),
) {
    use crate::ws_pump::{SyntheticSeriesKind::Text, UpstreamTerminalState, measurement_series};
    let series = measurement_series(Text, 1536, 199);
    let (aggregate, detail) = mode;
    let trace =
        FiniteTrace::configured(name, workers, 0, series.events.len(), false, false, detail);
    trace.set_series(series.delta_start, 1536);
    let limits = UpstreamInboundLimits::new(capacity, 16 * 1024 * 1024).unwrap();
    let (upstream, state, mut server) =
        measurement_stream_pair_with_limits(&trace, aggregate, limits).await;
    let mut body: ResponseBody = Box::pin(response_stream(state));
    let mut chunks = Vec::with_capacity(series.events.len() + 4);
    let mut supplied = 0;
    let mut quiescent = Vec::new();
    let mut overflow = false;
    let started = std::time::Instant::now();
    let result = measurement_attempt(&trace, async {
        for sequence in 0..series.delta_start {
            finite_send(
                &mut server,
                &trace,
                sequence,
                series.events[sequence].clone(),
            )
            .await;
            supplied += 1;
            matrix_take(&mut body, &trace, &mut chunks).await;
        }
        if preload == 128 {
            for _ in 0..24 {
                for sequence in supplied..supplied + 16 {
                    finite_send(
                        &mut server,
                        &trace,
                        sequence,
                        series.events[sequence].clone(),
                    )
                    .await;
                }
                supplied += 16;
                finite_read_barrier(&mut server, &trace).await;
                for _ in 0..8 {
                    matrix_take(&mut body, &trace, &mut chunks).await;
                }
                trace.event(
                    "consumer_ack",
                    Some(supplied - 1),
                    None,
                    Some(upstream.test_queue_sample()),
                    None,
                );
            }
            assert_eq!(upstream.test_queue_sample().0, 192);
            trace.event(
                "resume",
                None,
                None,
                Some(upstream.test_queue_sample()),
                None,
            );
            for _ in 0..192 {
                matrix_take(&mut body, &trace, &mut chunks).await;
            }
            assert_eq!(upstream.test_queue_sample().0, 0);
            quiescent.push(supplied);
        } else if preload > 0 {
            for sequence in series.delta_start..series.delta_start + preload {
                finite_send(
                    &mut server,
                    &trace,
                    sequence,
                    series.events[sequence].clone(),
                )
                .await;
                supplied += 1;
            }
            finite_read_barrier(&mut server, &trace).await;
            assert_eq!(upstream.test_queue_sample().0, preload);
            assert_eq!(upstream.terminal_state(), UpstreamTerminalState::Open);
            trace.event(
                "processing_gate_pending",
                None,
                None,
                Some(upstream.test_queue_sample()),
                None,
            );
            if preload == capacity {
                finite_send(
                    &mut server,
                    &trace,
                    supplied,
                    series.events[supplied].clone(),
                )
                .await;
                supplied += 1;
                while !upstream.is_closed() {
                    tokio::task::yield_now().await;
                }
                assert!(matches!(
                    upstream.terminal_state(),
                    UpstreamTerminalState::InboundBufferOverflow(_)
                ));
                assert!(
                    upstream
                        .diagnostic_activity()
                        .contains("cause=message_count")
                        || upstream
                            .diagnostic_output()
                            .1
                            .contains("cause=message_count")
                );
                overflow = true;
                trace.event(
                    "supply_stopped",
                    Some(supplied - 1),
                    None,
                    Some(upstream.test_queue_sample()),
                    None,
                );
            } else {
                trace.event(
                    "resume",
                    None,
                    None,
                    Some(upstream.test_queue_sample()),
                    None,
                );
                for _ in 0..preload {
                    matrix_take(&mut body, &trace, &mut chunks).await;
                }
                assert_eq!(upstream.test_queue_sample().0, 0);
                quiescent.push(supplied);
                trace.event(
                    "consumer_ack",
                    Some(supplied - 1),
                    None,
                    Some(upstream.test_queue_sample()),
                    None,
                );
            }
        }
        if !overflow {
            while supplied < series.events.len() {
                let end = (supplied + 8).min(series.events.len());
                for sequence in supplied..end {
                    finite_send(
                        &mut server,
                        &trace,
                        sequence,
                        series.events[sequence].clone(),
                    )
                    .await;
                }
                finite_read_barrier(&mut server, &trace).await;
                for _ in supplied..end {
                    matrix_take(&mut body, &trace, &mut chunks).await;
                }
                supplied = end;
                assert_eq!(upstream.test_queue_sample().0, 0);
                quiescent.push(supplied);
                trace.event(
                    "consumer_ack",
                    Some(supplied - 1),
                    None,
                    Some(upstream.test_queue_sample()),
                    None,
                );
            }
        } else {
            matrix_take(&mut body, &trace, &mut chunks).await;
        }
        matrix_take(&mut body, &trace, &mut chunks).await;
        assert!(body.next().await.is_none());
        trace.event("eof", None, None, None, None);
    })
    .await;
    let elapsed_ns = started.elapsed().as_nanos();
    drop(body);
    drop(server);
    let before_cleanup = upstream.test_queue_sample();
    let physically_drained = stop_measurement_pump(upstream, &trace).await;
    use futures_util::FutureExt;
    let validation = std::panic::AssertUnwindSafe(async {
        trace.assert_measurement_records(None);
        validate_matrix_output(&chunks, &series, Text, overflow)
    })
    .catch_unwind()
    .await;
    if validation.is_err() {
        trace.invalidate(6);
    }
    let delivered = validation
        .as_ref()
        .ok()
        .and_then(|value| value["delivered_inputs"].as_u64());
    trace.write_scenario(name, &json!({"matrix":if name.starts_with("observer") {"observer"} else if preload == capacity {"M3"} else if preload == 192 {"M4"} else if preload == 128 {"M6_quota"} else {"M2"},
        "generator":series.metadata,"runtime_flavor":if workers==2 {"multi_thread"} else {"current_thread"},"workers":workers,"mode":if detail {"C"} else if aggregate {"B"} else {"A"},"aggregate":aggregate,"detail":detail,
        "limits":{"messages":capacity,"bytes":16777216},"preload":preload,"batch_frames_max":8,"quiescent_Q0_after_input_count":quiescent,
        "supplied_inputs":supplied,"producer_complete":result.is_ok(),"consumer_end":result.is_ok(),"pump_end":"stop_and_join awaited","retained_ownership_released":true,
        "overflow":overflow,"elapsed_ns":elapsed_ns,"valid":result.is_ok() && validation.is_ok() && trace.invalid_reason().is_none(),"invalid_reason":trace.invalid_reason(),
        "output":validation.ok(),"delivered_sequence_inclusive":delivered.filter(|count| *count>0).map(|count| [0,count-1]),
        "undelivered_sequence_inclusive":delivered.filter(|count| (*count as usize)<series.events.len()).map(|count| [count,series.events.len() as u64-1]),
        "diagnostic_queue_before_cleanup":before_cleanup.0,"physical_drain_count":physically_drained,"quota":if preload==128 {Some(json!({"supply_frames":16,"drain_chunks":8,"rounds":24,"backlog_before_recovery":192}))} else {None},
        "cleanup":"producer future ended; body dropped; registry released; pump stop joined; all queued permits drained","additional_contrasts_used":0}));
    trace.flush();
    trace.assert_valid();
    assert!(result.is_ok());
    trace.assert_measurement_records(None);
}

#[tokio::test]
async fn response_stream_matrix_controlled_current_thread() {
    for (name, capacity, preload) in [
        ("m2_256_current", 256, 0),
        ("m2_512_current", 512, 0),
        ("m2_1024_current", 1024, 0),
        ("m3_256_current", 256, 256),
        ("m3_512_current", 512, 512),
        ("m3_1024_current", 1024, 1024),
        ("m4_current", 256, 192),
    ] {
        controlled_matrix_case(name, 1, capacity, preload, (true, true)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_stream_matrix_controlled_two_workers() {
    for (name, capacity, preload) in [
        ("m2_256_multi", 256, 0),
        ("m2_512_multi", 512, 0),
        ("m2_1024_multi", 1024, 0),
        ("m3_256_multi", 256, 256),
        ("m3_512_multi", 512, 512),
        ("m3_1024_multi", 1024, 1024),
        ("m4_multi", 256, 192),
    ] {
        controlled_matrix_case(name, 2, capacity, preload, (true, true)).await;
    }
}

struct NaturalCase {
    name: String,
    workers: usize,
    capacity: usize,
    kind: crate::ws_pump::SyntheticSeriesKind,
    size: usize,
    raw: bool,
    ready: bool,
    slow: bool,
}

struct NaturalOutput {
    chunks: Vec<Bytes>,
    raw: Vec<String>,
    pause_completed: bool,
}

async fn natural_matrix_case(case: NaturalCase) {
    use crate::ws_pump::{TraceOperation, UpstreamTerminalState, measurement_series};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let series = measurement_series(case.kind, 1536, case.size);
    let trace = FiniteTrace::configured(
        &case.name,
        case.workers,
        0,
        series.events.len(),
        true,
        false,
        true,
    );
    trace.set_series(series.delta_start, 1536);
    let limits = UpstreamInboundLimits::new(case.capacity, 16 * 1024 * 1024).unwrap();
    let (upstream, state, server, gate) = if case.ready {
        let (io, open, reader) = crate::ws_pump::measurement_ready_io(&trace, &series.events);
        let client = WebSocketStream::from_raw_socket(io, Role::Client, None).await;
        let upstream = Arc::new(LiveUpstreamWebSocket::test_measurement_stream(
            client,
            true,
            trace.clone(),
            limits,
        ));
        let registry = Arc::new(RetainedSessionRegistry::new(1));
        let lease = armed_followup_lease(&registry, upstream.clone()).await;
        let state = followup_stream_state(upstream.clone(), lease);
        (upstream, state, None, Some((open, reader)))
    } else {
        let (upstream, state, server) =
            measurement_stream_pair_with_limits(&trace, true, limits).await;
        (upstream, state, Some(server), None)
    };
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let consumer_trace = trace.clone();
    let consumer_upstream = upstream.clone();
    let pending_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let consumer_pending = pending_observed.clone();
    let raw = case.raw;
    let slow = case.slow;
    let frames = series.events.len();
    let mut consumer = tokio::spawn(async move {
        let mut output = NaturalOutput {
            chunks: Vec::new(),
            raw: Vec::new(),
            pause_completed: false,
        };
        let mut body: ResponseBody = Box::pin(response_stream(state));
        let mut pending_tx = Some(pending_tx);
        loop {
            let next = if raw {
                let receive = consumer_trace
                    .observe_operation(TraceOperation::RawReceive, consumer_upstream.recv_text());
                tokio::pin!(receive);
                let result = std::future::poll_fn(|context| {
                    let polled = std::future::Future::poll(receive.as_mut(), context);
                    if polled.is_pending()
                        && let Some(sender) = pending_tx.take()
                    {
                        consumer_pending.store(true, Ordering::SeqCst);
                        consumer_trace.event("consumer_pending", None, None, None, None);
                        let _ = sender.send(());
                    }
                    polled
                })
                .await;
                match result {
                    Ok(Some(text)) => {
                        let last = output.raw.len() == frames - 1;
                        assert!(output.raw.len() < 1543);
                        output.raw.push(text);
                        !last
                    }
                    Err(crate::ws_pump::UpstreamWebSocketError::InboundBufferOverflow) => false,
                    other => panic!(
                        "unexpected raw receive ending: {}",
                        if other.is_err() { "error" } else { "EOF" }
                    ),
                }
            } else {
                let receive =
                    consumer_trace.observe_operation(TraceOperation::BodyNext, body.next());
                tokio::pin!(receive);
                let result = std::future::poll_fn(|context| {
                    let polled = std::future::Future::poll(receive.as_mut(), context);
                    if polled.is_pending()
                        && let Some(sender) = pending_tx.take()
                    {
                        consumer_pending.store(true, Ordering::SeqCst);
                        consumer_trace.event("consumer_pending", None, None, None, None);
                        let _ = sender.send(());
                    }
                    polled
                })
                .await;
                match result {
                    Some(Ok(chunk)) => {
                        assert!(output.chunks.len() < 1547);
                        output.chunks.push(chunk);
                        true
                    }
                    None => false,
                    Some(Err(never)) => match never {},
                }
            };
            if !next {
                break;
            }
            let count = output.raw.len() + output.chunks.len();
            if slow && count == 8 {
                consumer_trace.event(
                    "processing_gate_pending",
                    None,
                    None,
                    Some(consumer_upstream.test_queue_sample()),
                    None,
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                consumer_trace.event(
                    "resume",
                    None,
                    None,
                    Some(consumer_upstream.test_queue_sample()),
                    None,
                );
                output.pause_completed = true;
            }
            if slow && count.is_multiple_of(8) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        consumer_trace.event("eof", None, None, None, None);
        output
    });
    let supplied = Arc::new(AtomicUsize::new(0));
    let supply_trace = trace.clone();
    let supply_upstream = upstream.clone();
    let supply_count = supplied.clone();
    let payloads = series.events.clone();
    let delta_start = series.delta_start;
    let mut producer = tokio::spawn(async move {
        pending_rx.await.unwrap();
        let started = std::time::Instant::now();
        if let Some((open, reader)) = gate {
            supply_trace.event(
                "input_gate_open",
                None,
                None,
                Some(supply_upstream.test_queue_sample()),
                None,
            );
            open.store(true, Ordering::SeqCst);
            reader.wake();
            supply_count.store(payloads.len(), Ordering::SeqCst);
            return (None, started.elapsed().as_nanos(), None);
        }
        let mut server = server.unwrap();
        let base = tokio::time::Instant::now();
        let scheduled_base = supply_trace.monotonic_ns().unwrap();
        let mut delta_times = None;
        for (sequence, text) in payloads.into_iter().enumerate() {
            if supply_upstream.is_closed() {
                break;
            }
            let batch = if sequence < delta_start {
                0
            } else {
                (sequence - delta_start).min(1535) / 16
            };
            let offset = std::time::Duration::from_millis(batch as u64 * 5);
            if sequence >= delta_start && (sequence - delta_start).is_multiple_of(16) {
                tokio::time::sleep_until(base + offset).await;
            }
            supply_trace.planned(
                sequence,
                text.len(),
                Some(scheduled_base + offset.as_nanos() as u64),
            );
            supply_trace.event("send_start", Some(sequence), Some(text.len()), None, None);
            let bytes = text.len();
            if supply_trace
                .observe_operation(TraceOperation::Send, server.send(Message::Text(text)))
                .await
                .is_err()
            {
                break;
            }
            supply_count.fetch_add(1, Ordering::SeqCst);
            supply_trace.event("send", Some(sequence), Some(bytes), None, None);
            if (delta_start..delta_start + 1536).contains(&sequence) {
                let now = supply_trace.monotonic_ns().unwrap();
                let times = delta_times.get_or_insert([now, now]);
                times[1] = now;
            }
        }
        (Some(server), started.elapsed().as_nanos(), delta_times)
    });
    let started = std::time::Instant::now();
    let mut consumer_joined = false;
    let mut producer_joined = false;
    let result = measurement_attempt(&trace, async {
        let output = (&mut consumer).await;
        consumer_joined = true;
        let output = output.unwrap();
        assert!(
            !slow || output.pause_completed,
            "required pause did not complete"
        );
        let producer_output = (&mut producer).await;
        producer_joined = true;
        let producer_output = producer_output.unwrap();
        assert!(
            producer_output.1 <= 2_000_000_000,
            "supply exceeded two seconds"
        );
        (output, producer_output)
    })
    .await;
    let elapsed_ns = started.elapsed().as_nanos();
    if result.is_err() {
        consumer.abort();
        producer.abort();
        if !consumer_joined {
            let _ = consumer.await;
        }
        if !producer_joined {
            let _ = producer.await;
        }
        trace.event("owner_cancelled", None, None, None, None);
    }
    let overflow = matches!(
        upstream.terminal_state(),
        UpstreamTerminalState::InboundBufferOverflow(_)
    );
    let before_cleanup = upstream.test_queue_sample();
    let mut validation = None;
    let mut supply = None;
    let mut pause_completed = false;
    if let Ok((output, (server, duration, times))) = result {
        pause_completed = output.pause_completed;
        drop(server);
        supply = Some(
            json!({"elapsed_ns":duration,"delta_send_window_ns":times.map(|times| times[1]-times[0]),
            "planned_delta_batch":16,"planned_batch_period_ns":5000000,"planned_send_rate_achieved":times.map(|times| supplied.load(Ordering::SeqCst) >= series.delta_start + 1536 && times[1]-times[0] <= 480000000),
            "interpretation":"unachieved natural rate is unidentified; quota control is not a rate proxy"}),
        );
        use futures_util::FutureExt;
        let checked = std::panic::AssertUnwindSafe(async {
            if raw {
                for (sequence, text) in output.raw.iter().enumerate() { assert_eq!(text, &series.events[sequence]); }
                if !overflow { assert_eq!(output.raw.len(), series.events.len()); }
                json!({"delivered_inputs":output.raw.len(),"raw_payload_bytes":output.raw.iter().map(String::len).collect::<Vec<_>>(),"completed":!overflow})
            } else { validate_matrix_output(&output.chunks, &series, case.kind, overflow) }
        }).catch_unwind().await;
        match checked {
            Ok(value) => validation = Some(value),
            Err(_) => trace.invalidate(6),
        }
    }
    let physically_drained = stop_measurement_pump(upstream, &trace).await;
    use futures_util::FutureExt;
    if std::panic::AssertUnwindSafe(async {
        trace.assert_measurement_records(None);
    })
    .catch_unwind()
    .await
    .is_err()
    {
        trace.invalidate(6);
    }
    let delivered = validation
        .as_ref()
        .and_then(|value| value["delivered_inputs"].as_u64());
    trace.write_scenario(&case.name, &json!({"matrix":if case.ready {"M5/M7"} else {"M6"},"generator":series.metadata,
        "runtime_flavor":if case.workers==2 {"multi_thread"} else {"current_thread"},"workers":case.workers,"mode":"C","aggregate":true,"detail":true,
        "limits":{"messages":case.capacity,"bytes":16777216},"consumer":if raw {"raw_recv"} else {"response_stream"},"transport":if case.ready {"finite_ready_encoded"} else {"duplex_paced"},
        "ready_has_no_send_time":case.ready,"first_actual_pending_before_gate":pending_observed.load(Ordering::SeqCst),"planned_slow_chunks_per_5ms":if slow {Some(8)} else {None},"planned_pause_ns":if slow {50000000} else {0},"actual_pause_completed":pause_completed,
        "supply":supply,"supplied_or_encoded_available_inputs":supplied.load(Ordering::SeqCst),"output":validation,"delivered_sequence_inclusive":delivered.filter(|count| *count>0).map(|count|[0,count-1]),
        "undelivered_sequence_inclusive":delivered.filter(|count| (*count as usize)<series.events.len()).map(|count|[count,series.events.len() as u64-1]),
        "diagnostic_queue_before_cleanup":before_cleanup.0,"physical_drain_count":physically_drained,"elapsed_ns":elapsed_ns,"valid":validation.is_some() && trace.invalid_reason().is_none(),"invalid_reason":trace.invalid_reason(),
        "producer_joined":true,"consumer_joined":true,"pump_end":"stop_and_join awaited","cleanup":"all owned tasks joined; body/lease released; queued byte permits drained",
        "overflow":overflow,"additional_contrasts_used":0,"missing_scope":if validation.is_none() {Some("consumer result unavailable after failure; full available numeric trace retained")} else {None}}));
    trace.flush();
    trace.assert_valid();
    assert!(validation.is_some());
    trace.assert_measurement_records(None);
}

async fn ready_matrix(workers: usize) {
    for capacity in [256, 512, 1024] {
        natural_matrix_case(NaturalCase {
            name: format!("m5_{capacity}_w{workers}"),
            workers,
            capacity,
            kind: crate::ws_pump::SyntheticSeriesKind::Text,
            size: 199,
            raw: false,
            ready: true,
            slow: false,
        })
        .await;
    }
}

#[tokio::test]
async fn response_stream_matrix_ready_current_thread() {
    ready_matrix(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_stream_matrix_ready_two_workers() {
    ready_matrix(2).await;
}

async fn paced_matrix(workers: usize) {
    for slow in [false, true] {
        natural_matrix_case(NaturalCase {
            name: format!("m6_slow{slow}_w{workers}"),
            workers,
            capacity: 256,
            kind: crate::ws_pump::SyntheticSeriesKind::Text,
            size: 199,
            raw: false,
            ready: false,
            slow,
        })
        .await;
    }
    controlled_matrix_case(
        &format!("m6_quota_w{workers}"),
        workers,
        256,
        128,
        (true, true),
    )
    .await;
}

#[tokio::test]
async fn response_stream_matrix_paced_current_thread() {
    paced_matrix(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_stream_matrix_paced_two_workers() {
    paced_matrix(2).await;
}

async fn paced_volume_matrix(workers: usize) {
    use crate::ws_pump::SyntheticSeriesKind::Arguments;
    for (size, raw) in [(199, false), (4096, false), (4096, true)] {
        natural_matrix_case(NaturalCase {
            name: format!("m7_paced_Arguments_{size}_raw{raw}_w{workers}"),
            workers,
            capacity: 256,
            kind: Arguments,
            size,
            raw,
            ready: false,
            slow: false,
        })
        .await;
    }
}

#[tokio::test]
async fn response_stream_matrix_paced_volume_current_thread() {
    paced_volume_matrix(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_stream_matrix_paced_volume_two_workers() {
    paced_volume_matrix(2).await;
}

async fn volume_matrix(workers: usize) {
    use crate::ws_pump::SyntheticSeriesKind::{Arguments, Text};
    for kind in [Text, Arguments] {
        for size in [199, 4096] {
            for raw in [true, false] {
                if matches!(kind, Text) && size == 199 && !raw {
                    continue;
                }
                natural_matrix_case(NaturalCase {
                    name: format!("m7_{kind:?}_{size}_raw{raw}_w{workers}"),
                    workers,
                    capacity: 256,
                    kind,
                    size,
                    raw,
                    ready: true,
                    slow: false,
                })
                .await;
            }
        }
    }
}

#[tokio::test]
async fn response_stream_matrix_volume_current_thread() {
    volume_matrix(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_stream_matrix_volume_two_workers() {
    volume_matrix(2).await;
}

async fn observer_matrix(workers: usize) {
    for repetition in 0..3 {
        let order = if repetition == 1 {
            [('C', true, true), ('B', true, false), ('A', false, false)]
        } else {
            [('A', false, false), ('B', true, false), ('C', true, true)]
        };
        for (mode, aggregate, detail) in order {
            controlled_matrix_case(
                &format!("observer_w{workers}_r{repetition}_{mode}"),
                workers,
                256,
                0,
                (aggregate, detail),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn response_stream_matrix_observer_current_thread() {
    observer_matrix(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_stream_matrix_observer_two_workers() {
    observer_matrix(2).await;
}

async fn start_sse_case(
    preload: usize,
) -> (
    FiniteTrace,
    Arc<LiveUpstreamWebSocket>,
    ResponseBody,
    WebSocketStream<DuplexStream>,
) {
    let trace = FiniteTrace::new(match preload {
        0 => "sse_alternating",
        192 => "sse_preloaded",
        _ => "sse_paused",
    });
    let (upstream, state, mut server) =
        diagnostic_stream_pair_with_trace(Some(trace.clone())).await;
    let mut body: ResponseBody = Box::pin(response_stream(state));
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    let created = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&created)["type"], "response.created");
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=awaiting_downstream_poll")
    );
    for ordinal in 1..=preload {
        finite_send(&mut server, &trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(&mut server, &trace).await;
    assert_eq!(upstream.test_queue_sample().0, preload);
    trace.event(
        "resume",
        None,
        None,
        Some(upstream.test_queue_sample()),
        None,
    );
    (trace, upstream, body, server)
}

async fn assert_sse_overflow(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    server: &mut WebSocketStream<DuplexStream>,
) {
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=awaiting_downstream_poll")
    );
    finite_send(server, trace, 257, synthetic_delta(257)).await;
    while !upstream.is_closed() {
        tokio::task::yield_now().await;
    }
    let frozen = upstream.diagnostic_output();
    assert_eq!(frozen.0, 1);
    assert!(frozen.1.contains("consumer_phase=awaiting_downstream_poll"));
    trace.diagnostic(&frozen.1);
    let failed = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&failed)["type"], "response.failed");
    assert!(
        std::str::from_utf8(&failed)
            .unwrap()
            .contains("upstream_inbound_buffer_overflow")
    );
    let done = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&done).unwrap(), "data: [DONE]\n\n");
    assert!(trace.observe(body.next()).await.is_none());
    assert_eq!(upstream.diagnostic_output(), frozen);
    trace.event(
        "outcome",
        Some(257),
        None,
        Some(upstream.test_queue_sample()),
        Some("expected_overflow"),
    );
}

async fn drain_preloaded_sse(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    preload: usize,
) {
    for ordinal in 1..=preload {
        let chunk = trace.observe(body.next()).await.unwrap().unwrap();
        let event = finite_sse_value(&chunk);
        assert_eq!(event["sequence_number"], ordinal);
        assert_eq!(event["delta"], format!("{ordinal:06}"));
    }
    assert_eq!(upstream.test_queue_sample().0, 0);
    trace.event(
        "recovered",
        None,
        None,
        Some(upstream.test_queue_sample()),
        None,
    );
}

async fn send_and_drain_sse_batches(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    server: &mut WebSocketStream<DuplexStream>,
    preload: usize,
) {
    for first in (preload + 1..=400).step_by(16) {
        for ordinal in first..first + 16 {
            finite_send(server, trace, ordinal, synthetic_delta(ordinal)).await;
        }
        finite_read_barrier(server, trace).await;
        for ordinal in first..first + 16 {
            let chunk = trace.observe(body.next()).await.unwrap().unwrap();
            let event = finite_sse_value(&chunk);
            assert_eq!(event["sequence_number"], ordinal);
            assert_eq!(event["delta"], format!("{ordinal:06}"));
        }
        assert_eq!(upstream.test_queue_sample().0, 0);
    }
}

async fn complete_sse_case(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    server: &mut WebSocketStream<DuplexStream>,
) {
    finite_send(server, trace, 401, synthetic_completed()).await;
    let completed = trace.observe(body.next()).await.unwrap().unwrap();
    let event = finite_sse_value(&completed);
    assert_eq!(event["type"], "response.completed");
    let expected: String = (1..=400).map(|ordinal| format!("{ordinal:06}")).collect();
    assert_eq!(
        event["response"]["output"][0]["content"][0]["text"],
        expected
    );
    let done = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&done).unwrap(), "data: [DONE]\n\n");
    assert!(trace.observe(body.next()).await.is_none());
    assert_eq!(
        upstream.terminal_state(),
        crate::ws_pump::UpstreamTerminalState::Open
    );
    trace.event(
        "outcome",
        Some(400),
        None,
        Some(upstream.test_queue_sample()),
        Some("not_reproduced"),
    );
}

async fn run_sse_case(preload: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (trace, upstream, mut body, mut server) = start_sse_case(preload).await;
        if preload == 256 {
            assert_sse_overflow(&trace, &upstream, &mut body, &mut server).await;
            return;
        }
        drain_preloaded_sse(&trace, &upstream, &mut body, preload).await;
        send_and_drain_sse_batches(&trace, &upstream, &mut body, &mut server, preload).await;
        complete_sse_case(&trace, &upstream, &mut body, &mut server).await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn response_stream_acknowledged_and_paused_batches_preserve_fifo_or_report_overflow() {
    for preload in [0, 192, 256] {
        run_sse_case(preload).await;
    }
}

async fn fill_default_queue(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    server: &mut WebSocketStream<DuplexStream>,
    terminal_at: tokio::time::Instant,
) {
    for ordinal in 1..=256 {
        finite_send(server, trace, ordinal, synthetic_delta(ordinal)).await;
    }
    finite_read_barrier(server, trace).await;
    assert_eq!(upstream.test_queue_sample(), (256, 256, 256));
    assert_eq!(tokio::time::Instant::now(), terminal_at);
    finite_send(server, trace, 257, synthetic_delta(257)).await;
    while !upstream.is_closed() {
        tokio::task::yield_now().await;
    }
    assert_eq!(tokio::time::Instant::now(), terminal_at);
}

fn assert_seventeen_millisecond_snapshot(frozen: &(usize, String)) {
    assert_eq!(frozen.0, 1);
    for field in [
        "terminal=inbound_buffer_overflow",
        "response_state=unknown",
        "last_classified_response_state=in_progress",
        "unclassified_data_count=257",
        "response_ambiguous=false",
        "consumer_phase=awaiting_upstream",
        "last_consumer_poll_age_ms=17",
        "last_dequeue_age_ms=17",
        "queued_messages=256",
        "queued_bytes=50944",
        "incoming_bytes=199",
        "queue_messages_high_water=256",
        "internal_tool_age_ms=-",
        "last_data_age_ms=0",
    ] {
        assert!(
            frozen.1.contains(field),
            "missing diagnostic field: {field}"
        );
    }
}

async fn finish_failed_sse(
    trace: &FiniteTrace,
    upstream: &LiveUpstreamWebSocket,
    body: &mut ResponseBody,
    frozen: &(usize, String),
    warning_count: &impl Fn() -> usize,
) {
    trace.diagnostic(&frozen.1);
    assert_eq!(warning_count(), 1);
    let failed = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&failed)["type"], "response.failed");
    let done = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&done).unwrap(), "data: [DONE]\n\n");
    assert!(trace.observe(body.next()).await.is_none());
    assert_eq!(upstream.test_drain_after_terminal(trace).await, 255);
    upstream.observe_response_event(crate::ws_pump::ResponseEvent::Progress);
    assert_eq!(upstream.diagnostic_output(), *frozen);
    assert_eq!(warning_count(), 1);
    trace.event(
        "outcome",
        Some(257),
        None,
        Some(upstream.test_queue_sample()),
        Some("expected_overflow"),
    );
}

async fn run_pending_receive_snapshot(warning_count: impl Fn() -> usize) {
    let trace = FiniteTrace::configured("sse_pending_seventeen_ms", 1, 0, 402, false, true, true);
    let (upstream, state, mut server) =
        diagnostic_stream_pair_with_trace(Some(trace.clone())).await;
    let mut body: ResponseBody = Box::pin(response_stream(state));
    let initial = tokio::time::Instant::now();
    finite_send(&mut server, &trace, 0, synthetic_created()).await;
    let created = trace.observe(body.next()).await.unwrap().unwrap();
    assert_eq!(finite_sse_value(&created)["type"], "response.created");
    assert_eq!(tokio::time::Instant::now(), initial);
    {
        let waiting = trace.observe(body.next());
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
    }
    assert!(
        upstream
            .diagnostic_activity()
            .contains("consumer_phase=awaiting_upstream")
    );
    tokio::time::advance(std::time::Duration::from_millis(17)).await;
    let terminal_at = initial + std::time::Duration::from_millis(17);
    assert_eq!(tokio::time::Instant::now(), terminal_at);
    fill_default_queue(&trace, &upstream, &mut server, terminal_at).await;
    let frozen = upstream.diagnostic_output();
    assert_seventeen_millisecond_snapshot(&frozen);
    finish_failed_sse(&trace, &upstream, &mut body, &frozen, &warning_count).await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn response_stream_pending_receive_preserves_seventeen_millisecond_overflow_snapshot() {
    let (_keepalive, _guard, warning_count) = finite_warning_capture();
    finite_drive(run_pending_receive_snapshot(warning_count)).await;
}
