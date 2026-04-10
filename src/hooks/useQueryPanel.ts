"use client";

import { useContext } from "react";
import { QueryPanelContext } from "../context/QueryPanelContext";

export function useQueryPanel() {
  const context = useContext(QueryPanelContext);
  if (!context) {
    throw new Error("useQueryPanel must be used within a QueryPanelProvider");
  }
  return context;
}
