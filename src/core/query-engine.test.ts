import { beforeEach, describe, expect, it, type Mock, vi } from "vitest";
import { type DatabaseMetadata, QueryEngine } from "./query-engine";

describe("QueryEngine", () => {
	let queryEngine: QueryEngine;
	let mockAdapter: {
		execute: Mock;
		validate: Mock;
		introspect: Mock;
		getDialect: Mock;
	};

	beforeEach(() => {
		queryEngine = new QueryEngine();
		mockAdapter = {
			execute: vi.fn(),
			validate: vi.fn(),
			introspect: vi.fn(),
			getDialect: vi.fn(() => "postgres"),
		};
	});

	describe("attachDatabase", () => {
		it("should attach database with metadata", () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			const adapter = queryEngine.getDatabase("test-db");
			expect(adapter).toBe(mockAdapter);
		});

		it("should set first attached database as default", () => {
			const metadata: DatabaseMetadata = {
				name: "db1",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("db1", mockAdapter, metadata);

			expect(queryEngine.getDefaultDatabase()).toBe("db1");
		});

		it("should not override default database when attaching second", () => {
			const metadata1: DatabaseMetadata = {
				name: "db1",
				dialect: "postgres",
			};
			const metadata2: DatabaseMetadata = {
				name: "db2",
				dialect: "clickhouse",
			};

			const mockAdapter2 = { ...mockAdapter };

			queryEngine.attachDatabase("db1", mockAdapter, metadata1);
			queryEngine.attachDatabase("db2", mockAdapter2, metadata2);

			expect(queryEngine.getDefaultDatabase()).toBe("db1");
		});
	});

	describe("getDatabase", () => {
		it("should return attached database", () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			const adapter = queryEngine.getDatabase("test-db");
			expect(adapter).toBe(mockAdapter);
		});

		it("should return default database when no name provided", () => {
			const metadata: DatabaseMetadata = {
				name: "default-db",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("default-db", mockAdapter, metadata);

			const adapter = queryEngine.getDatabase();
			expect(adapter).toBe(mockAdapter);
		});

		it("should throw error if database not found", () => {
			expect(() => queryEngine.getDatabase("non-existent")).toThrow(
				"Database 'non-existent' not found",
			);
		});

		it("should throw error if no database attached", () => {
			expect(() => queryEngine.getDatabase()).toThrow("No database attached");
		});
	});

	describe("getDatabaseMetadata", () => {
		it("should return metadata for attached database", () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
				description: "Test database",
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			const retrieved = queryEngine.getDatabaseMetadata("test-db");
			expect(retrieved).toEqual(metadata);
		});

		it("should return undefined if database not attached", () => {
			const metadata = queryEngine.getDatabaseMetadata("non-existent");
			expect(metadata).toBeUndefined();
		});
	});

	describe("validateAndExecute", () => {
		beforeEach(() => {
			mockAdapter.validate.mockResolvedValue();
			mockAdapter.execute.mockResolvedValue({
				rows: [{ id: 1, name: "test" }],
				fields: ["id", "name"],
			});
		});

		it("should validate and execute SQL", async () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			const result = await queryEngine.validateAndExecute(
				"SELECT * FROM users",
				{},
				"test-db",
				"tenant-1",
			);

			expect(mockAdapter.validate).toHaveBeenCalledWith(
				"SELECT * FROM users",
				{},
			);
			expect(mockAdapter.execute).toHaveBeenCalledWith(
				"SELECT * FROM users",
				{},
			);
			expect(result).toEqual({
				rows: [{ id: 1, name: "test" }],
				fields: ["id", "name"],
			});
		});

		it("should apply tenant isolation when configured", async () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
				tenantFieldName: "tenant_id",
				enforceTenantIsolation: true,
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			await queryEngine.validateAndExecute(
				"SELECT * FROM users",
				{},
				"test-db",
				"tenant-123",
			);

			// Should have modified SQL to include tenant isolation
			expect(mockAdapter.execute).toHaveBeenCalledWith(
				"SELECT * FROM users WHERE tenant_id = 'tenant-123'",
				{},
			);
		});

		it("should not apply tenant isolation when enforceTenantIsolation is false", async () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
				tenantFieldName: "tenant_id",
				enforceTenantIsolation: false,
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			await queryEngine.validateAndExecute(
				"SELECT * FROM users",
				{},
				"test-db",
				"tenant-123",
			);

			expect(mockAdapter.execute).toHaveBeenCalledWith(
				"SELECT * FROM users",
				{},
			);
		});

		it("should handle ClickHouse tenant isolation format", async () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "clickhouse",
				tenantFieldName: "tenant_id",
				tenantFieldType: "String",
				enforceTenantIsolation: true,
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			await queryEngine.validateAndExecute(
				"SELECT * FROM users",
				{},
				"test-db",
				"tenant-123",
			);

			expect(mockAdapter.execute).toHaveBeenCalledWith(
				"SELECT * FROM users WHERE tenant_id = {tenant_id:String}",
				{ tenant_id: "tenant-123" },
			);
		});

		it("should add tenant filter to existing WHERE clause", async () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
				tenantFieldName: "tenant_id",
				enforceTenantIsolation: true,
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			await queryEngine.validateAndExecute(
				"SELECT * FROM users WHERE active = true",
				{},
				"test-db",
				"tenant-123",
			);

			// Note: There's a double space due to the regex replacement
			expect(mockAdapter.execute).toHaveBeenCalledWith(
				"SELECT * FROM users WHERE tenant_id = 'tenant-123' AND  active = true",
				{},
			);
		});

		it("should not add tenant filter if already present", async () => {
			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
				tenantFieldName: "tenant_id",
				enforceTenantIsolation: true,
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			await queryEngine.validateAndExecute(
				"SELECT * FROM users WHERE tenant_id = 'other-tenant'",
				{},
				"test-db",
				"tenant-123",
			);

			// Should not modify SQL if tenant field already present
			expect(mockAdapter.execute).toHaveBeenCalledWith(
				"SELECT * FROM users WHERE tenant_id = 'other-tenant'",
				{},
			);
		});
	});

	describe("execute", () => {
		it("should execute SQL and return rows", async () => {
			mockAdapter.execute.mockResolvedValue({
				rows: [{ id: 1 }],
				fields: ["id"],
			});

			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			const result = await queryEngine.execute(
				"SELECT * FROM users",
				{ limit: 10 },
				"test-db",
			);

			expect(result).toEqual([{ id: 1 }]);
			expect(mockAdapter.execute).toHaveBeenCalledWith("SELECT * FROM users", {
				limit: 10,
			});
		});

		it("should return empty array on error", async () => {
			mockAdapter.execute.mockRejectedValue(new Error("Connection failed"));

			const metadata: DatabaseMetadata = {
				name: "test-db",
				dialect: "postgres",
			};

			queryEngine.attachDatabase("test-db", mockAdapter, metadata);

			const result = await queryEngine.execute("SELECT * FROM users");

			expect(result).toEqual([]);
		});
	});

	describe("mapGeneratedParams", () => {
		it("should map params with name field", () => {
			const params = [
				{ name: "userId", value: 123 },
				{ name: "active", value: true },
			];

			const result = queryEngine.mapGeneratedParams(params);

			expect(result).toEqual({
				userId: 123,
				active: true,
			});
		});

		it("should map params with placeholder field", () => {
			const params = [
				{ placeholder: "user_id", value: 123 },
				{ placeholder: "status", value: "active" },
			];

			const result = queryEngine.mapGeneratedParams(params);

			expect(result).toEqual({
				user_id: 123,
				status: "active",
			});
		});

		it("should map params with position field", () => {
			const params = [
				{ position: 1, value: "John" },
				{ position: 2, value: 25 },
			];

			const result = queryEngine.mapGeneratedParams(params);

			expect(result).toEqual({
				"1": "John",
				"2": 25,
			});
		});

		it("should use index as fallback", () => {
			const params = [{ value: "test" }, { value: 123 }];

			const result = queryEngine.mapGeneratedParams(params);

			expect(result).toEqual({
				"1": "test",
				"2": 123,
			});
		});

		it("should skip params without value", () => {
			const params = [
				{ name: "param1", value: "value1" },
				{ name: "param2" },
				{ name: "param3", value: "value3" },
			];

			const result = queryEngine.mapGeneratedParams(params);

			expect(result).toEqual({
				param1: "value1",
				param3: "value3",
			});
		});

		it("should strip special characters from param names", () => {
			const params = [
				{ name: "{userId}", value: 123 },
				{ name: "$status", value: "active" },
				{ name: ":count", value: 10 },
			];

			const result = queryEngine.mapGeneratedParams(params);

			expect(result).toEqual({
				userId: 123,
				status: "active",
				count: 10,
			});
		});
	});
});
