"use client";

import { useState, useEffect, useRef, useMemo } from "react";
import { DatabaseIcon, ChevronDownIcon } from "lucide-react";
import { runDedupedRequest } from "../utils/requestDedup";

type Datasource = {
  id: string;
  name: string;
  dialect: "postgres" | "clickhouse" | "bigquery";
  has_password?: boolean;
};

export interface DatasourceSelectorProps {
  /** Organization ID for the request header */
  organizationId: string;
  /** Currently selected datasource IDs */
  selectedIds: string[];
  /** Callback when selection changes */
  onSelectionChange: (ids: string[]) => void;
  /** Selection mode */
  selectionMode?: "single" | "multiple";
  /** URL to fetch datasources from (default: /api/datasources) */
  datasourcesUrl?: string;
  /** Optional extra headers for the fetch request */
  headers?: Record<string, string>;
  /** Whether to render dark theme styles */
  darkMode?: boolean;
  /** When set, only show these datasource IDs (e.g. dashboard's available_datasource_ids). Empty/undefined = show all. */
  allowedIds?: string[] | null;
  /** Stretch trigger and dropdown to 100% of the parent (e.g. modal settings column). */
  fullWidth?: boolean;
}

const EMPTY_HEADERS: Record<string, string> = {};

function getHeadersSignature(headers: Record<string, string>) {
  return JSON.stringify(
    Object.entries(headers).sort(([a], [b]) => a.localeCompare(b))
  );
}

export function DatasourceSelector({
  organizationId,
  selectedIds,
  onSelectionChange,
  selectionMode = "multiple",
  datasourcesUrl = "/api/datasources",
  headers,
  darkMode = false,
  allowedIds,
  fullWidth = false,
}: DatasourceSelectorProps) {
  const [datasources, setDatasources] = useState<Datasource[]>([]);
  const [loading, setLoading] = useState(true);
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const extraHeaders = headers ?? EMPTY_HEADERS;
  const headersSignature = getHeadersSignature(extraHeaders);

  useEffect(() => {
    const fetchDatasources = async () => {
      try {
        const response = await fetch(datasourcesUrl, {
          headers: {
            "Content-Type": "application/json",
            "x-organization-id": organizationId,
            ...extraHeaders,
          },
        });

        if (response.ok) {
          const data = await response.json();
          setDatasources(data.datasources || []);
        }
      } catch (error) {
        console.error("Failed to fetch datasources:", error);
      } finally {
        setLoading(false);
      }
    };

    if (organizationId) {
      const requestKey = `datasources:${datasourcesUrl}:${organizationId}:${headersSignature}`;
      void runDedupedRequest(requestKey, fetchDatasources);
    }
  }, [organizationId, datasourcesUrl, headersSignature, extraHeaders]);

  const filteredDatasources = useMemo(
    () =>
      allowedIds && allowedIds.length > 0
        ? datasources.filter((ds) => allowedIds.includes(ds.id))
        : datasources,
    [datasources, allowedIds]
  );

  // When only one datasource exists (after dashboard allowlist), select it by default.
  useEffect(() => {
    if (loading || !organizationId) return;
    if (filteredDatasources.length !== 1) return;
    if (selectedIds.length > 0) return;
    onSelectionChange([filteredDatasources[0].id]);
  }, [
    loading,
    organizationId,
    filteredDatasources,
    selectedIds.length,
    onSelectionChange,
  ]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleToggle = (id: string) => {
    const newSelection =
      selectionMode === "single"
        ? selectedIds.includes(id)
          ? []
          : [id]
        : selectedIds.includes(id)
          ? selectedIds.filter((sid) => sid !== id)
          : [...selectedIds, id];
    onSelectionChange(newSelection);
  };

  const selectedDatasources = filteredDatasources.filter((ds) => selectedIds.includes(ds.id));
  const colors = darkMode
    ? {
        mutedText: "#94a3b8",
        text: "#f8fafc",
        buttonBg: "#0f172a",
        panelBg: "#111827",
        border: "#334155",
        badgeBg: "#1e293b",
      }
    : {
        mutedText: "#64748b",
        text: "#0f172a",
        buttonBg: "#ffffff",
        panelBg: "#ffffff",
        border: "#e2e8f0",
        badgeBg: "#f1f5f9",
      };

  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          fontSize: "0.875rem",
          color: colors.mutedText,
          width: fullWidth ? "100%" : undefined,
          boxSizing: "border-box",
        }}
      >
        <DatabaseIcon size={16} style={{ animation: "pulse 1.5s ease-in-out infinite" }} />
        Loading datasources...
      </div>
    );
  }

  if (filteredDatasources.length === 0) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          fontSize: "0.875rem",
          color: colors.mutedText,
          width: fullWidth ? "100%" : undefined,
          boxSizing: "border-box",
        }}
      >
        <DatabaseIcon size={16} />
        {allowedIds && allowedIds.length > 0 ? "No allowed datasources for this dashboard" : "No datasources configured"}
      </div>
    );
  }

  const labelText =
    selectedDatasources.length > 0
      ? selectedDatasources.length === 1
        ? selectedDatasources[0].name
        : `${selectedDatasources.length} datasources`
      : "Select datasources";

  return (
    <div
      ref={containerRef}
      style={{
        position: "relative",
        width: fullWidth ? "100%" : undefined,
        boxSizing: "border-box",
      }}
    >
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          padding: "0.5rem 0.75rem",
          fontSize: "0.875rem",
          border: `1px solid ${colors.border}`,
          borderRadius: "0.375rem",
          background: colors.buttonBg,
          color: colors.text,
          cursor: "pointer",
          width: fullWidth ? "100%" : undefined,
          boxSizing: "border-box",
          minHeight: "2.5rem",
        }}
      >
        <DatabaseIcon size={16} style={{ flexShrink: 0 }} />
        <span
          style={{
            flex: fullWidth ? 1 : undefined,
            minWidth: fullWidth ? 0 : undefined,
            overflow: fullWidth ? "hidden" : undefined,
            textOverflow: fullWidth ? "ellipsis" : undefined,
            whiteSpace: fullWidth ? "nowrap" : undefined,
            textAlign: "left",
          }}
        >
          {labelText}
        </span>
        <ChevronDownIcon
          size={16}
          style={{
            marginLeft: fullWidth ? "auto" : "0.25rem",
            flexShrink: 0,
          }}
        />
      </button>

      {open && (
        <div
          style={{
            position: "absolute",
            top: "100%",
            left: 0,
            right: fullWidth ? 0 : undefined,
            marginTop: "0.25rem",
            minWidth: fullWidth ? undefined : "16rem",
            width: fullWidth ? "100%" : undefined,
            boxSizing: "border-box",
            padding: "0.5rem",
            background: colors.panelBg,
            border: `1px solid ${colors.border}`,
            borderRadius: "0.5rem",
            boxShadow: darkMode ? "0 10px 20px -6px rgba(0,0,0,0.45)" : "0 10px 15px -3px rgba(0,0,0,0.1)",
            zIndex: 50,
          }}
        >
          <div
            style={{
              padding: "0.5rem 0.75rem",
              fontSize: "0.75rem",
              fontWeight: 600,
              color: colors.mutedText,
              textTransform: "uppercase",
            }}
          >
            Data Sources
          </div>
          <div style={{ borderTop: `1px solid ${colors.border}`, margin: "0.25rem 0" }} />
          {filteredDatasources.map((ds) => (
            <label
              key={ds.id}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "0.5rem",
                padding: "0.5rem 0.75rem",
                cursor: "pointer",
                borderRadius: "0.25rem",
              }}
            >
              <input
                type={selectionMode === "single" ? "radio" : "checkbox"}
                checked={selectedIds.includes(ds.id)}
                onChange={() => handleToggle(ds.id)}
                style={{
                  margin: 0,
                  accentColor: "#2563eb",
                }}
              />
              <DatabaseIcon size={16} />
              <span style={{ flex: 1, fontSize: "0.875rem", color: colors.text }}>{ds.name}</span>
              <span
                style={{
                  fontSize: "0.625rem",
                  padding: "0.125rem 0.375rem",
                  background: colors.badgeBg,
                  color: colors.mutedText,
                  borderRadius: "0.25rem",
                }}
              >
                {ds.dialect}
              </span>
            </label>
          ))}
          {selectedDatasources.length > 0 && (
            <>
              <div style={{ borderTop: `1px solid ${colors.border}`, margin: "0.25rem 0" }} />
              <div
                style={{
                  padding: "0.375rem 0.75rem",
                  fontSize: "0.75rem",
                  color: colors.mutedText,
                }}
              >
                {selectedDatasources.length} selected
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
