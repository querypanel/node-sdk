import { beforeEach, describe, expect, it, vi } from "vitest";
import { createMockQueryPanelApi } from "../test-utils";
import {
	deleteSession,
	getSession,
	listSessions,
	updateSession,
} from "./sessions";

describe("routes/sessions", () => {
	let mockClient: ReturnType<typeof createMockQueryPanelApi>;

	beforeEach(() => {
		mockClient = createMockQueryPanelApi({
			get: vi.fn(),
			patch: vi.fn(),
			delete: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		});
	});

	it("listSessions should call GET with query params", async () => {
		const response = {
			data: [],
			pagination: {
				page: 1,
				limit: 10,
				total: 0,
				totalPages: 0,
				hasNext: false,
				hasPrev: false,
			},
		};

		mockClient.get.mockResolvedValue(response);

		const result = await listSessions(mockClient, {
			tenantId: "tenant-1",
			pagination: { page: 2, limit: 20 },
			sortBy: "updated_at",
			sortDir: "asc",
			title: "Revenue",
		});

		expect(result).toEqual(response);
		expect(mockClient.get).toHaveBeenCalledWith(
			"/sessions?page=2&limit=20&sort_by=updated_at&sort_dir=asc&title=Revenue",
			"tenant-1",
			undefined,
			undefined,
			undefined,
		);
	});

	it("getSession should include include_turns when provided", async () => {
		const session = {
			id: "session-1",
			session_id: "public-session-1",
			organization_id: "org-1",
			tenant_id: "tenant-1",
			user_id: null,
			title: "Session",
			created_at: "2025-01-01T00:00:00Z",
			updated_at: "2025-01-01T00:00:00Z",
			turns: [],
		};

		mockClient.get.mockResolvedValue(session);

		const result = await getSession(mockClient, "public-session-1", {
			tenantId: "tenant-1",
			includeTurns: true,
		});

		expect(result).toEqual(session);
		expect(mockClient.get).toHaveBeenCalledWith(
			"/sessions/public-session-1?include_turns=true",
			"tenant-1",
			undefined,
			undefined,
			undefined,
		);
	});

	it("updateSession should call PATCH", async () => {
		const session = { id: "session-1", session_id: "public-session-1" };
		mockClient.patch.mockResolvedValue(session);

		const result = await updateSession(
			mockClient,
			"public-session-1",
			{ title: "Updated" },
			{ tenantId: "tenant-1" },
		);

		expect(result).toEqual(session);
		expect(mockClient.patch).toHaveBeenCalledWith(
			"/sessions/public-session-1",
			{ title: "Updated" },
			"tenant-1",
			undefined,
			undefined,
			undefined,
		);
	});

	it("deleteSession should call DELETE", async () => {
		mockClient.delete.mockResolvedValue(undefined);

		await deleteSession(mockClient, "public-session-1", {
			tenantId: "tenant-1",
		});

		expect(mockClient.delete).toHaveBeenCalledWith(
			"/sessions/public-session-1",
			"tenant-1",
			undefined,
			undefined,
			undefined,
		);
	});
});
