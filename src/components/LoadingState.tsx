"use client";

import type { ThemeColors } from "../types";

export interface LoadingStateProps {
  /** Loading message */
  message?: string;
  /** Secondary message */
  submessage?: string;
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
  surface: "rgba(0,0,0,0.4)",
};

export function LoadingState({
  message = "Generating your visualization...",
  submessage = "AI is analyzing your data",
  colors = defaultColors,
  className = "",
  icon,
}: LoadingStateProps) {
  const mergedColors = { ...defaultColors, ...colors };

  const styles = {
    container: {
      position: "relative" as const,
      borderRadius: "1rem",
      overflow: "hidden" as const,
    },
    background: {
      position: "absolute" as const,
      inset: 0,
      background: `linear-gradient(135deg, ${mergedColors.primary}33, transparent, ${mergedColors.secondary}1a)`,
    },
    content: {
      position: "relative" as const,
      height: "450px",
      display: "flex",
      alignItems: "center" as const,
      justifyContent: "center" as const,
    },
    inner: {
      textAlign: "center" as const,
    },
    spinnerContainer: {
      position: "relative" as const,
      marginBottom: "1.5rem",
    },
    spinner: {
      width: "5rem",
      height: "5rem",
      borderRadius: "50%",
      border: `4px solid ${mergedColors.primary}33`,
      borderTopColor: mergedColors.primary,
      animation: "qp-loading-spin 1s linear infinite",
      margin: "0 auto",
    },
    iconContainer: {
      position: "absolute" as const,
      top: "50%",
      left: "50%",
      transform: "translate(-50%, -50%)",
      color: mergedColors.primary,
    },
    message: {
      fontSize: "1.25rem",
      fontWeight: 600,
      color: mergedColors.text,
      marginBottom: "0.5rem",
    },
    submessage: {
      color: mergedColors.muted,
    },
  };

  return (
    <div className={className} style={styles.container}>
      <style>
        {`@keyframes qp-loading-spin { to { transform: rotate(360deg); } }`}
      </style>
      <div style={styles.background} />
      <div style={styles.content}>
        <div style={styles.inner}>
          <div style={styles.spinnerContainer}>
            <div style={styles.spinner} />
            {icon && <div style={styles.iconContainer}>{icon}</div>}
          </div>
          <p style={styles.message}>{message}</p>
          <p style={styles.submessage}>{submessage}</p>
        </div>
      </div>
    </div>
  );
}
