"use client";

import { useState, useRef, type KeyboardEvent } from "react";
import type { ThemeColors, PromptChip } from "../types";

export interface QueryInputProps {
  /** Current query value */
  value?: string;
  /** Callback when query changes */
  onChange?: (value: string) => void;
  /** Callback when query is submitted */
  onSubmit?: (query: string) => void;
  /** Placeholder text */
  placeholder?: string;
  /** Whether input is disabled */
  disabled?: boolean;
  /** Whether query is loading */
  isLoading?: boolean;
  /** Prompt chips to display */
  chips?: PromptChip[];
  /** Callback when a chip is clicked */
  onChipClick?: (chip: PromptChip) => void;
  /** Theme colors */
  colors?: Partial<ThemeColors>;
  /** Additional class name */
  className?: string;
  /** Custom submit button content */
  submitLabel?: React.ReactNode;
  /** Custom loading indicator */
  loadingIndicator?: React.ReactNode;
}

const defaultColors: Partial<ThemeColors> = {
  primary: "#8B5CF6",
  secondary: "#3B82F6",
  text: "#F1F5F9",
  muted: "#94A3B8",
  border: "rgba(139,92,246,0.2)",
  surface: "rgba(0,0,0,0.5)",
  background: "#0a0612",
};

export function QueryInput({
  value: controlledValue,
  onChange,
  onSubmit,
  placeholder = "Ask a question about your data...",
  disabled = false,
  isLoading = false,
  chips = [],
  onChipClick,
  colors = defaultColors,
  className = "",
  submitLabel = "Ask",
  loadingIndicator,
}: QueryInputProps) {
  const [internalValue, setInternalValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const value = controlledValue ?? internalValue;
  const mergedColors = { ...defaultColors, ...colors };

  const handleChange = (newValue: string) => {
    if (controlledValue === undefined) {
      setInternalValue(newValue);
    }
    onChange?.(newValue);
  };

  const handleSubmit = () => {
    if (value.trim() && !disabled && !isLoading) {
      onSubmit?.(value);
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      handleSubmit();
    }
  };

  const handleChipClick = (chip: PromptChip) => {
    handleChange(chip.text);
    onChipClick?.(chip);
    onSubmit?.(chip.text);
  };

  const styles = {
    wrapper: {
      position: "relative" as const,
    },
    glowBg: {
      position: "absolute" as const,
      inset: 0,
      background: `linear-gradient(to right, ${mergedColors.primary}33, ${mergedColors.secondary}26)`,
      borderRadius: "1rem",
      filter: "blur(24px)",
    },
    container: {
      position: "relative" as const,
      backgroundColor: mergedColors.surface,
      backdropFilter: "blur(24px)",
      border: `1px solid ${mergedColors.border}`,
      borderRadius: "1rem",
      padding: "0.5rem",
      boxShadow: `0 25px 50px -12px ${mergedColors.primary}1a`,
    },
    inputRow: {
      display: "flex",
      gap: "0.5rem",
    },
    inputWrapper: {
      flex: 1,
      position: "relative" as const,
    },
    input: {
      width: "100%",
      height: "3.5rem",
      paddingLeft: "1rem",
      paddingRight: "1rem",
      backgroundColor: "transparent",
      color: mergedColors.text,
      fontSize: "1.125rem",
      border: "none",
      outline: "none",
      opacity: disabled ? 0.5 : 1,
    },
    button: {
      height: "3.5rem",
      padding: "0 2rem",
      background: `linear-gradient(to right, ${mergedColors.primary}, ${mergedColors.secondary})`,
      color: "white",
      fontWeight: 600,
      borderRadius: "0.75rem",
      border: "none",
      cursor: disabled || isLoading || !value.trim() ? "not-allowed" : "pointer",
      opacity: disabled || !value.trim() ? 0.5 : 1,
      transition: "all 0.15s",
      display: "flex",
      alignItems: "center",
      gap: "0.5rem",
      boxShadow: `0 10px 15px -3px ${mergedColors.primary}40`,
    },
    chipsContainer: {
      display: "flex",
      flexWrap: "wrap" as const,
      justifyContent: "center" as const,
      gap: "0.75rem",
      marginTop: "1rem",
    },
    chip: {
      display: "inline-flex",
      alignItems: "center",
      gap: "0.5rem",
      padding: "0.75rem 1.25rem",
      borderRadius: "0.75rem",
      backgroundColor: `${mergedColors.primary}0d`,
      border: `1px solid ${mergedColors.border}`,
      color: mergedColors.muted,
      fontSize: "0.875rem",
      fontWeight: 500,
      cursor: disabled || isLoading ? "not-allowed" : "pointer",
      opacity: disabled || isLoading ? 0.5 : 1,
      transition: "all 0.15s",
    },
    spinner: {
      width: "1.25rem",
      height: "1.25rem",
      border: "2px solid rgba(255,255,255,0.3)",
      borderTopColor: "white",
      borderRadius: "50%",
      animation: "qp-spin 1s linear infinite",
    },
  };

  return (
    <div className={className}>
      <style>
        {`@keyframes qp-spin { to { transform: rotate(360deg); } }`}
      </style>
      <div style={styles.wrapper}>
        <div style={styles.glowBg} />
        <div style={styles.container}>
          <div style={styles.inputRow}>
            <div style={styles.inputWrapper}>
              <input
                ref={inputRef}
                type="text"
                placeholder={placeholder}
                value={value}
                onChange={(e) => handleChange(e.target.value)}
                onKeyDown={handleKeyDown}
                disabled={disabled || isLoading}
                style={styles.input}
              />
            </div>
            <button
              type="button"
              onClick={handleSubmit}
              disabled={disabled || isLoading || !value.trim()}
              style={styles.button}
            >
              {isLoading ? (
                loadingIndicator ?? <div style={styles.spinner} />
              ) : (
                submitLabel
              )}
            </button>
          </div>
        </div>
      </div>

      {chips.length > 0 && (
        <div style={styles.chipsContainer}>
          {chips.map((chip) => (
            <button
              key={chip.key}
              type="button"
              onClick={() => handleChipClick(chip)}
              disabled={disabled || isLoading}
              style={styles.chip}
              onMouseEnter={(e) => {
                if (!disabled && !isLoading) {
                  e.currentTarget.style.backgroundColor = `${mergedColors.primary}26`;
                  e.currentTarget.style.borderColor = `${mergedColors.primary}66`;
                  e.currentTarget.style.color = mergedColors.text ?? "#F1F5F9";
                }
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = `${mergedColors.primary}0d`;
                e.currentTarget.style.borderColor = mergedColors.border ?? "";
                e.currentTarget.style.color = mergedColors.muted ?? "#94A3B8";
              }}
            >
              {chip.emoji && <span>{chip.emoji}</span>}
              <span>{chip.text}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
