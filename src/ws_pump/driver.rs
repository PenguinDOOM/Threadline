use super::*;

pub(super) struct OwnedPumpState {
    pub(super) inbound_tx: mpsc::Sender<InboundEnvelope>,
    pub(super) byte_budget: Arc<Semaphore>,
    pub(super) limits: UpstreamInboundLimits,
    pub(super) terminal_state: Arc<StdMutex<UpstreamTerminalState>>,
    pub(super) watchdog_policy: UpstreamWatchdogPolicy,
    pub(super) diagnostics: CloseDiagnostics,
}

impl OwnedPumpState {
    pub(super) fn borrowed(&mut self) -> PumpState<'_> {
        PumpState {
            inbound_tx: &self.inbound_tx,
            byte_budget: &self.byte_budget,
            limits: self.limits,
            terminal_state: &self.terminal_state,
            watchdog_policy: self.watchdog_policy,
            diagnostics: &mut self.diagnostics,
        }
    }
}

pub(super) async fn run_pump<S>(
    stream: WebSocketStream<S>,
    mut outbound_rx: mpsc::Receiver<OutboundCommand>,
    mut owned: OwnedPumpState,
    ping_interval: Duration,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut writer, mut reader) = stream.split();
    let mut schedule = PumpSchedule::new(ping_interval);
    debug!(
        outbound_capacity = OUTBOUND_CHANNEL_CAPACITY,
        ping_interval_secs = ping_interval.as_secs_f64(),
        "ws_pump_started"
    );
    let mut state = owned.borrowed();
    while check_liveness_deadline(
        state.terminal_state,
        schedule.pending_challenge.as_ref(),
        None,
        state.watchdog_policy,
        state.diagnostics,
    ) {
        if let Some(keep_running) =
            dispatch_control(&mut writer, &mut reader, &mut state, &mut schedule).await
        {
            if !keep_running {
                break;
            }
            continue;
        }
        let event = wait_for_event(&mut reader, &mut outbound_rx, &state, &schedule).await;
        if !handle_waiting_event(event, &mut writer, &mut reader, &mut state, &mut schedule).await {
            break;
        }
    }
    record_close(
        &owned.terminal_state,
        UpstreamCloseMetadata {
            code: None,
            reason: None,
            error: None,
        },
        UpstreamCloseSource::PumpExitFallback,
        &owned.diagnostics,
    );
}

struct PumpSchedule {
    next_ping_due: Instant,
    next_challenge: u64,
    pending_challenge: Option<PendingPongChallenge>,
    control_flush_needed: bool,
    prefer_inbound: bool,
    ping_interval: Duration,
}

impl PumpSchedule {
    fn new(ping_interval: Duration) -> Self {
        Self {
            next_ping_due: Instant::now() + ping_interval,
            next_challenge: 0,
            pending_challenge: None,
            control_flush_needed: false,
            prefer_inbound: false,
            ping_interval,
        }
    }

    fn liveness(&mut self) -> PumpLivenessState<'_> {
        PumpLivenessState {
            pending_challenge: &mut self.pending_challenge,
            control_flush_needed: &mut self.control_flush_needed,
            next_ping_due: &mut self.next_ping_due,
            ping_interval: self.ping_interval,
        }
    }

    fn ping_sent(&mut self) {
        if let Some(challenge) = self.pending_challenge.as_mut() {
            if challenge.acknowledged_early {
                self.pending_challenge = None;
                self.next_ping_due = Instant::now() + self.ping_interval;
            } else {
                challenge.sent_at = Some(Instant::now());
            }
        }
    }
}

async fn dispatch_control<S>(
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    reader: &mut SplitStream<WebSocketStream<S>>,
    pump_state: &mut PumpState<'_>,
    schedule: &mut PumpSchedule,
) -> Option<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if Instant::now() >= schedule.next_ping_due && schedule.pending_challenge.is_none() {
        return Some(dispatch_due_ping(writer, reader, pump_state, schedule).await);
    }
    if schedule.control_flush_needed {
        schedule.control_flush_needed = false;
        return Some(
            drive_write_operation(
                writer,
                reader,
                pump_state,
                Message::Pong(Vec::new()),
                UpstreamOutboundKind::ControlFlush,
                schedule.liveness(),
            )
            .await,
        );
    }
    None
}

async fn dispatch_due_ping<S>(
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    reader: &mut SplitStream<WebSocketStream<S>>,
    pump_state: &mut PumpState<'_>,
    schedule: &mut PumpSchedule,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let nonce = next_watchdog_nonce(&mut schedule.next_challenge);
    schedule.pending_challenge = Some(PendingPongChallenge {
        nonce: nonce.clone(),
        sent_at: None,
        acknowledged_early: false,
    });
    if !drive_write_operation(
        writer,
        reader,
        pump_state,
        Message::Ping(nonce),
        UpstreamOutboundKind::Ping,
        schedule.liveness(),
    )
    .await
    {
        return false;
    }
    schedule.ping_sent();
    true
}

enum WaitingEvent {
    Timer,
    Inbound(Option<Result<Message, TungsteniteError>>),
    Outbound(Option<OutboundCommand>),
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn wait_for_event<S>(
    reader: &mut SplitStream<WebSocketStream<S>>,
    outbound_rx: &mut mpsc::Receiver<OutboundCommand>,
    pump_state: &PumpState<'_>,
    schedule: &PumpSchedule,
) -> WaitingEvent
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let pong_deadline = schedule
        .pending_challenge
        .as_ref()
        .and_then(|challenge| challenge.sent_at)
        .map(|sent_at| sent_at + pump_state.watchdog_policy.pong_timeout());
    let ping_due = schedule
        .pending_challenge
        .is_none()
        .then_some(schedule.next_ping_due);
    if schedule.prefer_inbound {
        tokio::select! {
            biased;
            _ = wait_for_deadline(pong_deadline) => WaitingEvent::Timer,
            _ = wait_for_deadline(ping_due) => WaitingEvent::Timer,
            inbound = reader.next() => WaitingEvent::Inbound(inbound),
            outbound = outbound_rx.recv() => WaitingEvent::Outbound(outbound),
        }
    } else {
        tokio::select! {
            biased;
            _ = wait_for_deadline(pong_deadline) => WaitingEvent::Timer,
            _ = wait_for_deadline(ping_due) => WaitingEvent::Timer,
            outbound = outbound_rx.recv() => WaitingEvent::Outbound(outbound),
            inbound = reader.next() => WaitingEvent::Inbound(inbound),
        }
    }
}

async fn handle_waiting_event<S>(
    event: WaitingEvent,
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    reader: &mut SplitStream<WebSocketStream<S>>,
    pump_state: &mut PumpState<'_>,
    schedule: &mut PumpSchedule,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match event {
        WaitingEvent::Timer => true,
        WaitingEvent::Inbound(inbound) => {
            schedule.prefer_inbound = false;
            handle_inbound_and_schedule(inbound, pump_state, schedule.liveness())
        }
        WaitingEvent::Outbound(Some(OutboundCommand::Text(text))) => {
            schedule.prefer_inbound = true;
            drive_write_operation(
                writer,
                reader,
                pump_state,
                Message::Text(text),
                UpstreamOutboundKind::Text,
                schedule.liveness(),
            )
            .await
        }
        WaitingEvent::Outbound(None) => {
            record_close(
                pump_state.terminal_state,
                outbound_channel_closed_metadata(),
                UpstreamCloseSource::OutboundChannelClosed,
                pump_state.diagnostics,
            );
            false
        }
    }
}

pub(super) fn handle_inbound_and_schedule(
    inbound: Option<Result<Message, TungsteniteError>>,
    pump_state: &mut PumpState<'_>,
    liveness_state: PumpLivenessState<'_>,
) -> bool {
    let challenge_was_pending = liveness_state.pending_challenge.is_some();
    if !handle_inbound_message(
        inbound,
        pump_state,
        liveness_state.pending_challenge,
        liveness_state.control_flush_needed,
    ) {
        return false;
    }
    if challenge_was_pending && liveness_state.pending_challenge.is_none() {
        *liveness_state.next_ping_due = Instant::now() + liveness_state.ping_interval;
    }
    true
}
