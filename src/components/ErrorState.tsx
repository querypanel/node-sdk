"use client";

import type { ThemeColors } from "../types";

export interface ErrorStateProps {
  /** Error title */
  title?: string;
  /** Error message */
  message: string;
  /** Additional help text */
  helpText?: string;
  /** Theme colors */
  colors?: Partial<ThemeColors>;
  /** Additional class name */
  className?: string;
  /** Retry callback */
  onRetry?: () => void;
  /** Custom icon */
  icon?: React.ReactNode;
}

const defaultColors: Partial<ThemeColors> = {
  primary: "#8B5CF6",
  text: "#F1F5F9",
  muted: "#94A3B8",
  border: "rgba(139,92,246,0.2)",
  error: "#EF4444",
};

export function ErrorState({
  title = "Something went wrong",
  message,
  helpText,
  colors = defaultColors,
  className = "",
  onRetry,
  icon,
}: ErrorStateProps) {
  const mergedColors = { ...defaultColors, ...colors };

  const styles = {
    container: {
      borderRadius: "1rem",
      backgroundColor: "rgba(127, 29, 29, 0.2)",
      border: `1px solid rgba(239, 68, 68, 0.3)`,
      padding: "1.5rem",
    },
    content: {
      display: "flex",
      alignItems: "flex-start",
      gap: "1rem",
    },
    iconBox: {
      width: "3rem",
      height: "3rem",
      borderRadius: "50%",
      backgroundColor: "rgba(239, 68, 68, 0.2)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      flexShrink: 0,
    },
    iconDefault: {
      width: "1.5rem",
      height: "1.5rem",
      color: mergedColors.error,
    },
    textContainer: {
      flex: 1,
    },
    title: {
      fontWeight: 600,
      color: mergedColors.error,
      fontSize: "1.125rem",
    },
    message: {
      color: "rgba(252, 165, 165, 0.8)",
      marginTop: "0.25rem",
    },
    helpText: {
      color: mergedColors.muted,
      fontSize: "0.875rem",
      marginTop: "0.75rem",
    },
    retryButton: {
      marginTop: "1rem",
      padding: "0.5rem 1rem",
      borderRadius: "0.5rem",
      backgroundColor: "rgba(239, 68, 68, 0.2)",
      border: `1px solid rgba(239, 68, 68, 0.3)`,
      color: mergedColors.error,
      fontSize: "0.875rem",
      fontWeight: 500,
      cursor: "pointer",
      transition: "all 0.15s",
    },
  };

  const defaultIcon = (
    <svg
      style={styles.iconDefault}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
    >
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="8" x2="12" y2="12" />
      <line x1="12" y1="16" x2="12.01" y2="16" />
    </svg>
  );

  return (
    <div className={className} style={styles.container}>
      <div style={styles.content}>
        <div style={styles.iconBox}>{icon ?? defaultIcon}</div>
        <div style={styles.textContainer}>
          <h3 style={styles.title}>{title}</h3>
          <p style={styles.message}>{message}</p>
          {helpText && <p style={styles.helpText}>{helpText}</p>}
          {onRetry && (
            <button
              type="button"
              onClick={onRetry}
              style={styles.retryButton}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = "rgba(239, 68, 68, 0.3)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = "rgba(239, 68, 68, 0.2)";
              }}
            >
              Try again
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
