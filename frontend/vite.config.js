import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // PORT env override (e.g. preview tooling); defaults to vite's 5173
    port: Number(process.env.PORT) || 5173,
  },
})
