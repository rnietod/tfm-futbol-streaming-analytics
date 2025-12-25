import { nextui } from "@nextui-org/react";

/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
    "./node_modules/@nextui-org/theme/dist/**/*.{js,ts,jsx,tsx}", // Necesario para que NextUI parsee las clases
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "sans-serif"], // Limpieza para UI
        mono: ["JetBrains Mono", "monospace"], // Precisión para datos/tiempos
      },
      // Hemos eliminado 'colors.cyber'. Ahora confiamos en el sistema semántico.
    },
  },
  darkMode: "class", // Obligatorio para NextUI
  plugins: [
    nextui({
      prefix: "nextui",
      addCommonColors: true,
      defaultTheme: "dark",
      defaultExtendTheme: "dark",
      layout: {
        radius: {
          small: "4px", // Botones pequeños
          medium: "8px", // Inputs / Cards standard
          large: "12px", // Paneles grandes (Tactix Glass style)
        },
      },
      themes: {
        dark: {
          colors: {
            background: "#09090b", // Zinc-950: Profundidad premium
            foreground: "#ECEDEE", // Texto legible pero no blanco quemado
            primary: {
              DEFAULT: "#006FEE", // Azul Eléctrico (Acción)
              foreground: "#FFFFFF",
            },
            success: {
              DEFAULT: "#17c964", // Verde Datos Positivos
              foreground: "#000000",
            },
            danger: {
              DEFAULT: "#f31260", // Rojo Alerta
              foreground: "#FFFFFF",
            },
            focus: "#006FEE",
          },
        },
      },
    }),
  ],
};