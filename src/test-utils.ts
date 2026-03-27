import { vi, type Mock } from "vitest";
import type { IQueryPanelApi } from "./core/api-types";

/**
 * Test utilities and fixtures
 */

/** Mock API client: IQueryPanelApi with Mock methods so tests can use .mockResolvedValue etc. Assignable to IQueryPanelApi. */
export type MockQueryPanelApi = IQueryPanelApi & {
	getDefaultTenantId: Mock<() => string | undefined>;
	get: Mock<(...args: unknown[]) => Promise<unknown>>;
	post: Mock<(...args: unknown[]) => Promise<unknown>>;
	postWithHeaders: Mock<(...args: unknown[]) => Promise<{ data: unknown; headers: Headers }>>;
	put: Mock<(...args: unknown[]) => Promise<unknown>>;
	patch: Mock<(...args: unknown[]) => Promise<unknown>>;
	delete: Mock<(...args: unknown[]) => Promise<unknown>>;
};

/**
 * Creates a mock that satisfies IQueryPanelApi. Override the methods you need in tests.
 * Return type is MockQueryPanelApi so you can use mockClient.post.mockResolvedValue(...) etc.
 */
export function createMockQueryPanelApi(
	overrides?: Partial<IQueryPanelApi>,
): MockQueryPanelApi {
	return {
		getDefaultTenantId: vi.fn(),
		get: vi.fn(),
		post: vi.fn(),
		postWithHeaders: vi.fn(),
		put: vi.fn(),
		patch: vi.fn(),
		delete: vi.fn(),
		...overrides,
	} as MockQueryPanelApi;
}

/**
 * Valid RSA private key for testing JWT generation
 */
export const TEST_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCdtLNwj0X3L4Sc
kzUpX7siX2bNP0lhGk+quL+8Y+gTx9grbfx+HYnUre6TM6FjJvWCumqamXq8jyVr
KDrmOgWvEsGk/cx7Gi0kj3R4PMvLxgcAb08Am5o3rlD7LABKOd9aOhhvhv/9addc
Gph8jiDXnofh/x7AjGRj/Ox4zHiYKV3quMeJ9SgYn3vl26MU+u5CablHdINzRoeQ
L8Y5kqucsFEpBGAnfT4y3qhdqiomYCRlqiwgRlqT2w7pbqOrf4Rhdo3i2W/hS9eA
7qicx96KgsSGORJPzuP6sbRvZhnuKL6tx+gv4xCzg2squJ+PnWsXTdpXEeWRVUcA
gGUHqoGNAgMBAAECggEACmHbkFT2U7QepFoynUMupDAoqel5MLnVNdWM1d+z5x14
dz956GmUadp7gthazxbBAfa4zu28vU4lNf1Dh2WZezGeavjEbfRGtYn2LnY+Km+i
hV5OkqkryW+h6C8446oUsLFza2A1WR8PLQKZOFX5BojqxC+R/DYh481CWh+H7uhr
u13mTE15P6kcEhaM5eV0nVJfXsbv9BnU6WP7QWOlQcKot1LIwj6h5nqOfZ0vSeNi
2+fdkgCbu1C4EM99W00l1wRP+4Fkc96RSROdS+0KHp8moeHRblNPnJHUgwORtRem
VZ4UkhuD5neK1+rdWV3Vy/r+XdHyPjfnqwq6NmppRwKBgQDZTpRgvdPwEEC77Yig
C3mVNNY+pw7iZhyiwYc3cmID35yepM6xxiIywo/2GhjNTyjKw2QgLRD4Z2Qss8/H
HfHGjJef6cEqUFjxW43l/I4gX+eqcvevvRwbzCdFRu79neslMfmulQknYS+5+YLj
1kwSwB3ZqGz6qkcIVm6HkmcdTwKBgQC5yVj2YCdqk/axTXZn6NIK5IRlhi6zePzb
oHj+AkOD2Ltg/dD8Mjh5Oyz3YPzLScBts45vwk5ZfFT7GUFXCEmWTgYAaEQXzbTa
8+2ohCPnqVvOZEevhZIuZKC9QH22mqAR6tsPUVSOVS2reAN9li49JYHOBXZObUq2
s6iXDIAUYwKBgGwMYEaB3IF+81D92HMLgZaowOZCFmqHrzFV3K+7FfUrGCrhc0x9
Yb+vcflCHJbpBzVaVcyR2+BHOE3p0iQFRZQdj76ZccxiSvcnOLTkEJ8UUJI1u+YL
t5AywEv36xj7jlHeOXGO9RA5iIuF/ojeotCn6a+xyQ4R4GPK5gZOyyqNAoGBALO+
1/FWSgxVJ0GW/k299SlQub83uQsurQUYFrc6HXP8Rh2qs3ysYfL4i6KzFSDhM+lu
5Qm/Zeox3k4puwunGvrudeQC/I5DGQ0VHwQBBHPnaMkgQDLS+gEknlc7g+UdaGyt
Wk9Rkos6YbsZm9bva8EA/rsCkdmC59wnqZ6qG2idAoGAM2HfrwkM4Kz80bl5Sdqv
VtJ/Gh+BSV6G4F4qp1eZDuAU8KDXNrci8BChvw6QiJHjATvkKfL8whSwyk2/HMQ0
M/sTYxPqaBX5C6H6wwH/dayKJzAquhTWGc8GcaYljQRztqeI6+3fiDwusBuH0P4b
S5AHC7G7jkLu5OPtKEnCw38=
-----END PRIVATE KEY-----`;

export const TEST_ORG_ID = "org-test-123";
export const TEST_BASE_URL = "https://api.example.com";
