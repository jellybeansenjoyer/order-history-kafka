/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_SUPPORT_API_URL: string;
  readonly VITE_CHECKOUT_API_URL: string;
  readonly VITE_FRAUD_API_URL: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
