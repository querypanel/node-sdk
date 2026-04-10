/* eslint-disable react-refresh/only-export-components */
"use client";

import {
  createContext,
  useState,
  useCallback,
  useMemo,
  type ReactNode,
} from "react";
import type {
  QueryPanelConfig,
  QueryResult,
  ColorPreset,
  Theme,
  SqlModifications,
  VizModifications,
} from "../types";
import { defaultTheme, getColorsByPreset, createTheme } from "../themes";

export interface QueryPanelContextValue {
  // Config
  config: QueryPanelConfig;
  theme: Theme;
  colorPreset: ColorPreset;
  setColorPreset: (preset: ColorPreset) => void;

  // Query state
  query: string;
  setQuery: (query: string) => void;
  result: QueryResult | null;
  isLoading: boolean;
  error: string | null;

  // Actions
  ask: (question: string) => Promise<void>;
  modify: (options: {
    sqlModifications?: SqlModifications;
    vizModifications?: VizModifications;
  }) => Promise<void>;
  reset: () => void;
}

export const QueryPanelContext = createContext<QueryPanelContextValue | null>(null);

export interface QueryPanelProviderProps {
  children: ReactNode;
  config: QueryPanelConfig;
}

export function QueryPanelProvider({
  children,
  config,
}: QueryPanelProviderProps) {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [colorPreset, setColorPreset] = useState<ColorPreset>(
    config.colorPreset ?? "default"
  );

  const theme = useMemo(() => {
    const baseTheme = config.theme
      ? createTheme(config.theme)
      : defaultTheme;
    return {
      ...baseTheme,
      colors: {
        ...baseTheme.colors,
        ...getColorsByPreset(colorPreset),
      },
    };
  }, [config.theme, colorPreset]);

  const fetcher = config.fetcher ?? fetch;

  const ask = useCallback(
    async (question: string) => {
      if (!question.trim()) return;

      setIsLoading(true);
      setError(null);
      setQuery(question);

      try {
        const response = await fetcher(config.askEndpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ question }),
        });

        const data = (await response.json()) as QueryResult;

        if (!response.ok || !data.success) {
          throw new Error(data.error || "Failed to process query");
        }

        setResult(data);
      } catch (err) {
        const errorMessage =
          err instanceof Error ? err.message : "Unknown error occurred";
        setError(errorMessage);
      } finally {
        setIsLoading(false);
      }
    },
    [config.askEndpoint, fetcher]
  );

  const modify = useCallback(
    async (options: {
      sqlModifications?: SqlModifications;
      vizModifications?: VizModifications;
    }) => {
      if (!result?.sql || !config.modifyEndpoint) return;

      setIsLoading(true);
      setError(null);

      try {
        const response = await fetcher(config.modifyEndpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sql: result.sql,
            question: query,
            params: result.params,
            sqlModifications: options.sqlModifications,
            vizModifications: options.vizModifications,
          }),
        });

        const data = (await response.json()) as QueryResult;

        if (!response.ok || !data.success) {
          throw new Error(data.error || "Failed to modify chart");
        }

        setResult(data);
      } catch (err) {
        const errorMessage =
          err instanceof Error ? err.message : "Unknown error occurred";
        setError(errorMessage);
      } finally {
        setIsLoading(false);
      }
    },
    [config.modifyEndpoint, fetcher, query, result]
  );

  const reset = useCallback(() => {
    setQuery("");
    setResult(null);
    setError(null);
  }, []);

  const value = useMemo(
    () => ({
      config,
      theme,
      colorPreset,
      setColorPreset,
      query,
      setQuery,
      result,
      isLoading,
      error,
      ask,
      modify,
      reset,
    }),
    [
      config,
      theme,
      colorPreset,
      query,
      result,
      isLoading,
      error,
      ask,
      modify,
      reset,
    ]
  );

  return (
    <QueryPanelContext.Provider value={value}>
      {children}
    </QueryPanelContext.Provider>
  );
}

// Re-export hook from separate file for fast refresh compatibility
export { useQueryPanel } from "../hooks/useQueryPanel";
