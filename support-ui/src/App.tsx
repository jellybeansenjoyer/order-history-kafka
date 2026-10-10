import { useEffect, useState } from "react";
import {
  api,
  formatMoney,
  formatTime,
  type FraudScore,
  type OrderSummary,
  type TimelineEvent,
} from "./api";

export default function App() {
  const [orders, setOrders] = useState<OrderSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [timeline, setTimeline] = useState<TimelineEvent[]>([]);
  const [simRunning, setSimRunning] = useState(false);
  const [supportStats, setSupportStats] = useState<Record<string, unknown> | null>(null);
  const [fraudUp, setFraudUp] = useState(false);
  const [fraudStats, setFraudStats] = useState<Record<string, unknown> | null>(null);
  const [scores, setScores] = useState<FraudScore[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    try {
      const [orderRes, stats, health, fStats, fScores] = await Promise.all([
        api.listOrders(),
        api.supportStats(),
        api.checkoutHealth(),
        api.fraudStats(),
        api.fraudScores(),
      ]);
      setOrders(orderRes.orders);
      setSupportStats(stats);
      setSimRunning(Boolean(health.running));
      setFraudUp(fStats !== null);
      setFraudStats(fStats);
      setScores(fScores?.scores ?? []);
      setError(null);

      setSelectedId((prev) => {
        if (prev && orderRes.orders.some((o) => o.order_id === prev)) return prev;
        return orderRes.orders[0]?.order_id ?? null;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Refresh failed");
    }
  }

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => void refresh(), 2000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    if (!selectedId) {
      setTimeline([]);
      return;
    }
    let cancelled = false;
    void api
      .timeline(selectedId)
      .then((res) => {
        if (!cancelled) setTimeline(res.timeline);
      })
      .catch(() => {
        if (!cancelled) setTimeline([]);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedId, orders]);

  async function run(action: () => Promise<unknown>) {
    setBusy(true);
    try {
      await action();
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Action failed");
    } finally {
      setBusy(false);
    }
  }

  const selected = selectedId ? orders.find((o) => o.order_id === selectedId) : undefined;
  const selectedTerminal =
    selected?.status === "CANCELLED" || selected?.status === "DELIVERED";
  const selectedScore = selectedId
    ? scores.find((s) => s.order_id === selectedId)
    : undefined;

  return (
    <div className="app">
      <header className="hero">
        <div className="brand">Order Timeline · Late Consumer Replay</div>
        <h1>Support rebuilds history from Kafka — fraud joins later</h1>
        <p>
          Checkout only writes keyed <code>order-events</code>. Support projects timelines by
          consuming the log. Start the fraud scorer anytime; it backfills the same orders from
          earliest without touching the producer.
        </p>
      </header>

      <div className="controls">
        <button
          className="primary"
          disabled={busy || simRunning}
          onClick={() => void run(() => api.startSim())}
        >
          Start simulator
        </button>
        <button disabled={busy || !simRunning} onClick={() => void run(() => api.stopSim())}>
          Stop simulator
        </button>
        <button
          disabled={busy}
          onClick={() => void run(() => api.createOrder({ high_risk: false }))}
        >
          Create order
        </button>
        <button
          disabled={busy}
          onClick={() =>
            void run(() =>
              api.createOrder({ high_risk: true, amount_cents: 89999, force_address_change: true }),
            )
          }
        >
          Create risky order
        </button>
        <button
          disabled={busy || !selectedId || selectedTerminal}
          onClick={() => selectedId && void run(() => api.advanceOrder(selectedId))}
          title={
            selectedTerminal
              ? "Selected order is already terminal (CANCELLED / DELIVERED). Create a new order."
              : "Emit the next event for the selected order"
          }
        >
          Advance selected
        </button>
        <button
          className="danger"
          disabled={busy}
          onClick={() => void run(() => api.resetReplay())}
          title="Wipe support projection and replay from earliest"
        >
          Reset &amp; replay support
        </button>

        <span className={`pill ${simRunning ? "ok" : "warn"}`}>
          simulator {simRunning ? "running" : "idle"}
        </span>
        <span className="pill ok">
          support events {String(supportStats?.events_consumed ?? 0)} · orders{" "}
          {String(supportStats?.distinct_orders ?? 0)}
        </span>
        <span className={`pill ${fraudUp ? "ok" : "down"}`}>
          fraud {fraudUp ? `up · scored ${String(fraudStats?.orders_scored ?? 0)}` : "not running"}
        </span>
      </div>

      {!fraudUp && (
        <div className="hint">
          <strong>Demo wow — late join:</strong> leave Support running, then start fraud in another
          terminal:
          <br />
          <code>docker compose --profile fraud up -d --build fraud-scorer</code>
          <br />
          Fraud reads <code>order-events</code> from earliest and scores orders Support already saw.
          Checkout is unchanged.
        </div>
      )}

      {error && <div className="hint" style={{ borderColor: "var(--danger)", color: "var(--danger)" }}>{error}</div>}

      <div className="layout">
        <section className="panel">
          <header>
            <h2>Orders</h2>
            <span>{orders.length}</span>
          </header>
          <div className="body">
            {orders.length === 0 ? (
              <div className="empty">No orders yet. Start the simulator or create an order.</div>
            ) : (
              orders.map((o) => (
                <button
                  key={o.order_id}
                  className={`order-row ${selectedId === o.order_id ? "active" : ""}`}
                  onClick={() => setSelectedId(o.order_id)}
                >
                  <div className="id">{o.order_id}</div>
                  <div className="meta">
                    <span>{formatMoney(o.amount_cents, o.currency)}</span>
                    <span className={`status ${o.status}`}>{o.status}</span>
                  </div>
                  <div className="meta">
                    <span>{o.event_count} events</span>
                    <span>{formatTime(o.last_event_at)}</span>
                  </div>
                </button>
              ))
            )}
          </div>
        </section>

        <section className="panel">
          <header>
            <h2>Timeline {selectedId ? `· ${selectedId}` : ""}</h2>
            <span>replay projection</span>
          </header>
          <div className="body">
            {!selectedId || timeline.length === 0 ? (
              <div className="empty">Select an order to see its Kafka-rebuilt event timeline.</div>
            ) : (
              <>
                {selectedScore && (
                  <div className="fraud-card" style={{ margin: "0.5rem" }}>
                    <div className="top">
                      <strong>Fraud score (late consumer)</strong>
                      <span className={`score risk-${selectedScore.risk}`}>
                        {selectedScore.score} · {selectedScore.risk}
                      </span>
                    </div>
                    <ul className="reasons">
                      {selectedScore.reasons.map((r) => (
                        <li key={r}>{r}</li>
                      ))}
                    </ul>
                  </div>
                )}
                <ul className="timeline">
                  {timeline.map((e) => (
                    <li key={e.event_id}>
                      <div className="when">{formatTime(e.occurred_at)}</div>
                      <div className="what">
                        {e.event_type} → <span className={`status ${e.status}`}>{e.status}</span>
                      </div>
                      <div className="detail">
                        {formatMoney(e.amount_cents, e.currency)}
                        {e.metadata?.shipping_region
                          ? ` · region ${String(e.metadata.shipping_region)}`
                          : ""}
                        {e.metadata?.device_id ? ` · ${String(e.metadata.device_id)}` : ""}
                      </div>
                    </li>
                  ))}
                </ul>
              </>
            )}
          </div>
        </section>

        <section className="panel">
          <header>
            <h2>Fraud scorer</h2>
            <span>
              {fraudUp
                ? `group ${String(fraudStats?.consumer_group ?? "fraud-scorer")}`
                : "offline"}
            </span>
          </header>
          <div className="body">
            {!fraudUp ? (
              <div className="empty">
                Fraud is not running. When you start it with the compose <code>fraud</code> profile,
                it joins with <code>auto.offset.reset=earliest</code> and backfills every order
                already on the topic.
              </div>
            ) : scores.length === 0 ? (
              <div className="empty">
                Fraud is up — backfilling… events consumed:{" "}
                {String(fraudStats?.events_consumed ?? 0)}
              </div>
            ) : (
              scores.map((s) => (
                <button
                  key={s.order_id}
                  className="fraud-card"
                  style={{ width: "100%", textAlign: "left" }}
                  onClick={() => setSelectedId(s.order_id)}
                >
                  <div className="top">
                    <span className="id" style={{ fontFamily: "var(--mono)" }}>
                      {s.order_id}
                    </span>
                    <span className={`score risk-${s.risk}`}>
                      {s.score} · {s.risk}
                    </span>
                  </div>
                  <ul className="reasons">
                    {s.reasons.slice(0, 3).map((r) => (
                      <li key={r}>{r}</li>
                    ))}
                  </ul>
                </button>
              ))
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
