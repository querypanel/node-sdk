"use client";

import type { CSSProperties } from "react";
import type { ThemeColors } from "../types";

export interface EmptyStateProps {
  /** Title text */
  title?: string;
  /** Description text */
  description?: string;
  /** Feature badges to display */
  features?: Array<{ label: string; color?: string }>;
  /** Theme colors */
  colors?: Partial<ThemeColors>;
  /** Additional class name */
  className?: string;
  /** Custom icon */
  icon?: React.ReactNode;
}

const defaultColors: Partial<ThemeColors> = {
  primary: "#8B5CF6",
  secondary: "#3B82F6",
  text: "#F1F5F9",
  muted: "#94A3B8",
  border: "rgba(139,92,246,0.2)",
};

const defaultFeatures = [
  { label: "AI-powered", color: "#8B5CF6" },
  { label: "Real-time", color: "#10B981" },
  { label: "Interactive", color: "#3B82F6" },
];

export function EmptyState({
  title = "Ready to explore",
  description = "Ask anything about your data. Type a question or try one of the example prompts.",
  features = defaultFeatures,
  colors = defaultColors,
  className = "",
  icon,
}: EmptyStateProps) {
  const mergedColors = { ...defaultColors, ...colors };

  const container: CSSProperties = {
    position: "relative",
    borderRadius: "1rem",
    overflow: "hidden",
  };
  const background: CSSProperties = {
    position: "absolute",
    inset: 0,
    background: `linear-gradient(135deg, ${mergedColors.primary}26, transparent, ${mergedColors.secondary}1a)`,
  };
  const content: CSSProperties = {
    position: "relative",
    height: "450px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    border: `1px dashed ${mergedColors.border}`,
    borderRadius: "1rem",
  };
  const inner: CSSProperties = {
    textAlign: "center",
    maxWidth: "28rem",
    margin: "0 auto",
    padding: "0 1.5rem",
  };
  const iconBox: CSSProperties = {
    width: "6rem",
    height: "6rem",
    borderRadius: "1rem",
    background: `linear-gradient(135deg, ${mergedColors.primary}33, ${mergedColors.secondary}33)`,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    margin: "0 auto 2rem",
    border: `1px solid ${mergedColors.border}`,
    boxShadow: `0 25px 50px -12px ${mergedColors.primary}1a`,
  };
  const iconDefault: CSSProperties = {
    width: "3rem",
    height: "3rem",
    color: mergedColors.primary,
  };
  const titleStyle: CSSProperties = {
    fontSize: "1.5rem",
    fontWeight: 700,
    color: mergedColors.text,
    marginBottom: "0.75rem",
  };
  const descriptionStyle: CSSProperties = {
    color: mergedColors.muted,
    marginBottom: "1.5rem",
    lineHeight: 1.6,
  };
  const featuresStyle: CSSProperties = {
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "1rem",
    fontSize: "0.875rem",
    color: mergedColors.muted,
  };
  const feature: CSSProperties = {
    display: "flex",
    alignItems: "center",
    gap: "0.375rem",
  };
  const featureDot: CSSProperties = {
    width: "0.5rem",
    height: "0.5rem",
    borderRadius: "50%",
  };

  const defaultIconEl = (
    <svg
      style={iconDefault}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
    >
      <rect x="2" y="2" width="20" height="20" rx="2.18" ry="2.18" />
      <line x1="7" y1="2" x2="7" y2="22" />
      <line x1="17" y1="2" x2="17" y2="22" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <line x1="2" y1="7" x2="7" y2="7" />
      <line x1="2" y1="17" x2="7" y2="17" />
      <line x1="17" y1="17" x2="22" y2="17" />
      <line x1="17" y1="7" x2="22" y2="7" />
    </svg>
  );

  return (
    <div className={className} style={container}>
      <div style={background} />
      <div style={content}>
        <div style={inner}>
          <div style={iconBox}>{icon ?? defaultIconEl}</div>
          <h3 style={titleStyle}>{title}</h3>
          <p style={descriptionStyle}>{description}</p>
          <div style={featuresStyle}>
            {features.map((f, idx) => (
              <span key={idx} style={feature}>
                <span
                  style={{
                    ...featureDot,
                    backgroundColor: f.color ?? mergedColors.primary,
                  }}
                />
                {f.label}
              </span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
