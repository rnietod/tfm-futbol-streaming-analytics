/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        cyber: {
          dark: '#0b1116',   // Fondo principal (Void)
          panel: '#151f2b',  // Paneles secundarios
          neon: '#00f2ff',   // Cyan Neón (Acento)
          green: '#10b981',  // Verde Datos (Positivo)
          red: '#ef4444',    // Rojo Alerta (Negativo)
          text: '#e2e8f0'    // Texto principal
        }
      },
      fontFamily: {
        mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', 'monospace'],
        sans: ['system-ui', '-apple-system', 'Segoe UI', 'Roboto', 'sans-serif'],
      },
      boxShadow: {
        'neon': '0 0 5px theme("colors.cyber.neon"), 0 0 10px theme("colors.cyber.neon")',
      }
    },
  },
  plugins: [],
}