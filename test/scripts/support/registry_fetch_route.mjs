// Route the real CLI's registry HTTP requests to an owned loopback server.
// Keep fetch, response parsing, auth retries and the CLI filesystem boundary real.
const loopback = new URL(process.env.TEST_REGISTRY_ORIGIN);
if (loopback.hostname !== "127.0.0.1" || loopback.protocol !== "http:") {
	throw new Error("registry fixture must use loopback HTTP");
}
const networkFetch = globalThis.fetch;
globalThis.fetch = (input, options) => {
	const url = new URL(input);
	if (url.origin !== "https://ghcr.io")
		throw new Error("unexpected registry origin");
	return networkFetch(new URL(url.pathname + url.search, loopback), options);
};
