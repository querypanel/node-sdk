"use client";

import { createContext, useContext } from "react";

export interface ThemeContextValue {
  darkMode: boolean;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({
  darkMode,
  children,
}: {
  darkMode: boolean;
  children: React.ReactNode;
}) {
  return (
    <ThemeContext.Provider value={{ darkMode }}>{children}</ThemeContext.Provider>
  );
}

export function useThemeContext(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  return ctx ?? { darkMode: true };
}
