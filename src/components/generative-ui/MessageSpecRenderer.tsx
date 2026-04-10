"use client";

import { useMemo } from "react";
import type { UIMessage } from "ai";
import { autoFixSpec } from "@json-render/core";
import { Renderer, useJsonRenderMessage } from "@json-render/react";
import { registry } from "./registry";
import { GenerativeUIProvider } from "./provider";

export function MessageSpecRenderer({
  parts,
  queryResultBaseUrl = "/api/query-results",
}: {
  parts: UIMessage["parts"];
  queryResultBaseUrl?: string;
}) {
  const { hasSpec, spec } = useJsonRenderMessage(parts);

  const fixedSpec = useMemo(
    () => (spec ? autoFixSpec(spec).spec : null),
    [spec]
  );

  if (!hasSpec || !fixedSpec) return null;

  return (
    <GenerativeUIProvider queryResultBaseUrl={queryResultBaseUrl}>
      <Renderer spec={fixedSpec} registry={registry} />
    </GenerativeUIProvider>
  );
}
