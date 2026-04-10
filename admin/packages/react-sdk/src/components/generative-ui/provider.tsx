"use client";

import React from "react";
import {
  ActionProvider,
  StateProvider,
  VisibilityProvider,
} from "@json-render/react";

type GenerativeUIConfig = {
  queryResultBaseUrl: string;
};

const GenerativeUIConfigContext = React.createContext<GenerativeUIConfig>({
  queryResultBaseUrl: "/api/query-results",
});

export function GenerativeUIProvider({
  children,
  queryResultBaseUrl = "/api/query-results",
}: {
  children: React.ReactNode;
  queryResultBaseUrl?: string;
}) {
  return (
    <GenerativeUIConfigContext.Provider value={{ queryResultBaseUrl }}>
      <StateProvider>
        <ActionProvider handlers={{}}>
          <VisibilityProvider>{children}</VisibilityProvider>
        </ActionProvider>
      </StateProvider>
    </GenerativeUIConfigContext.Provider>
  );
}

export function useGenerativeUIConfig() {
  return React.useContext(GenerativeUIConfigContext);
}
