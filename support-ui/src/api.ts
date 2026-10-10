const SUPPORT = import.meta.env.VITE_SUPPORT_API_URL || "http://localhost:8102";
const CHECKOUT = import.meta.env.VITE_CHECKOUT_API_URL || "http://localhost:8101";
const FRAUD = import.meta.env.VITE_FRAUD_API_URL || "http://localhost:8103";

export type OrderSummary = {
  order_id: string;
  customer_id: string;
  amount_cents: number;
  currency: string;
  status: string;
  event_count: number;
  first_event_at: string;
  last_event_at: string;
};

export type TimelineEvent = {
  event_id: string;
  order_id: string;
  event_type: string;
  status: string;
  occurred_at: string;
  customer_id: string;
  amount_cents: number;
  currency: string;
  metadata: Record<string, unknown>;
};

export type FraudScore = {
  order_id: string;
  score: number;
  risk: string;
  reasons: string[];
  event_count: number;
  amount_cents?: number;
  last_status?: string;
  last_event_at?: string;
};

async function errorMessage(res: Response): Promise<string> {
  try {
    const data = (await res.json()) as { detail?: unknown };
    if (typeof data.detail === "string") return data.detail;
  } catch {
    /* ignore non-JSON bodies */
  }
  return `${res.status} ${res.statusText}`;
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(await errorMessage(res));
  }
  return res.json() as Promise<T>;
}

async function postJson<T>(url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(await errorMessage(res));
  }
  return res.json() as Promise<T>;
}

export const api = {
  supportUrl: SUPPORT,
  checkoutUrl: CHECKOUT,
  fraudUrl: FRAUD,

  listOrders: () => getJson<{ count: number; orders: OrderSummary[] }>(`${SUPPORT}/orders`),
  timeline: (orderId: string) =>
    getJson<{ order_id: string; event_count: number; timeline: TimelineEvent[]; source: string }>(
      `${SUPPORT}/orders/${orderId}/timeline`,
    ),
  supportStats: () => getJson<Record<string, unknown>>(`${SUPPORT}/stats`),
  resetReplay: () => postJson<Record<string, unknown>>(`${SUPPORT}/admin/reset-and-replay`),

  checkoutHealth: () => getJson<Record<string, unknown>>(`${CHECKOUT}/health`),
  startSim: () => postJson<Record<string, unknown>>(`${CHECKOUT}/simulate/start`, { interval_ms: 1500 }),
  stopSim: () => postJson<Record<string, unknown>>(`${CHECKOUT}/simulate/stop`),
  createOrder: (opts?: { high_risk?: boolean; amount_cents?: number; force_address_change?: boolean }) =>
    postJson<TimelineEvent>(`${CHECKOUT}/orders`, opts ?? {}),
  advanceOrder: (orderId: string) => postJson<TimelineEvent>(`${CHECKOUT}/orders/${orderId}/advance`),

  fraudHealth: async (): Promise<Record<string, unknown> | null> => {
    try {
      return await getJson(`${FRAUD}/health`);
    } catch {
      return null;
    }
  },
  fraudStats: async (): Promise<Record<string, unknown> | null> => {
    try {
      return await getJson(`${FRAUD}/stats`);
    } catch {
      return null;
    }
  },
  fraudScores: async (): Promise<{ count: number; scores: FraudScore[] } | null> => {
    try {
      return await getJson(`${FRAUD}/scores`);
    } catch {
      return null;
    }
  },
};

export function formatMoney(cents: number, currency = "USD"): string {
  return new Intl.NumberFormat("en-US", { style: "currency", currency }).format(cents / 100);
}

export function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return iso;
  }
}
