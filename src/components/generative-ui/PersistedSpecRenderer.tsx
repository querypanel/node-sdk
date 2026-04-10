"use client";

import { useMemo } from "react";
import { autoFixSpec, type Spec } from "@json-render/core";
import { Renderer } from "@json-render/react";
import { registry } from "./registry";
import { GenerativeUIProvider } from "./provider";

export function PersistedSpecRenderer({
  spec,
  queryResultBaseUrl = "/api/query-results",
}: {
  spec: unknown;
  queryResultBaseUrl?: string;
}) {
  const fixedSpec = useMemo(() => {
    if (!spec || typeof spec !== "object") return null;
    return autoFixSpec(spec as Spec).spec;
  }, [spec]);

  if (!fixedSpec) return null;

  return (
    <GenerativeUIProvider queryResultBaseUrl={queryResultBaseUrl}>
      <Renderer spec={fixedSpec} registry={registry} />
    </GenerativeUIProvider>
  );
}
