import assert from "node:assert/strict";
import test from "node:test";
import {
	SavedResetConnection,
	savedResetConnectParams,
} from "./saved_reset_connection.mjs";

const fixture = () => {
	const notice = { hidden: true };
	const enabled = {
		dataset: { serverDisabled: "false" },
		disabled: false,
		setAttribute(name, value) {
			this[name] = value;
		},
	};
	const denied = {
		dataset: { serverDisabled: "true" },
		disabled: true,
		setAttribute(name, value) {
			this[name] = value;
		},
	};
	const listeners = new Map();
	const el = {
		dataset: {},
		querySelector: () => notice,
		querySelectorAll: (selector) =>
			selector.includes("notice") ? [notice] : [enabled, denied],
		addEventListener: (name, fn) => listeners.set(name, fn),
		removeEventListener: (name, fn) => {
			if (listeners.get(name) === fn) listeners.delete(name);
		},
		contains: () => true,
	};
	const socket = {
		connected: true,
		isConnected() {
			return this.connected;
		},
	};
	const hook = { ...SavedResetConnection, el, liveSocket: socket };
	hook.mounted();
	return { hook, el, notice, enabled, denied, listeners, socket };
};

test("actual disconnect callback disables only reset controls and explains independent work", () => {
	const f = fixture();
	f.socket.connected = false;
	f.hook.disconnected();
	assert.equal(f.enabled.disabled, true);
	assert.equal(f.denied.disabled, true);
	assert.equal(f.notice.hidden, false);
	assert.equal(f.el.dataset.savedResetConnectionState, "disconnected");
});

test("socket open alone and intervening patches never unlock before native rejoin completion", () => {
	const f = fixture();
	f.hook.disconnected();
	f.socket.connected = true;
	f.enabled.disabled = false;
	f.hook.updated();
	assert.equal(f.enabled.disabled, true);
	assert.equal(f.notice.hidden, false);
	f.hook.reconnected();
	assert.equal(f.enabled.disabled, false);
	assert.equal(f.denied.disabled, true);
	assert.equal(f.notice.hidden, true);
});

test("click, keyboard activation and submit are stopped even before disconnect callback", () => {
	const f = fixture();
	f.socket.connected = false;
	for (const type of ["click", "keydown", "submit"]) {
		const event = {
			type,
			key: "Enter",
			target: { closest: () => f.enabled },
			preventDefault() {
				this.prevented = true;
			},
			stopImmediatePropagation() {
				this.stopped = true;
			},
		};
		f.listeners.get(type)(event);
		assert.equal(event.prevented, true);
		assert.equal(event.stopped, true);
	}
});

test("repeated interruptions keep fresh server denial and destroy removes listeners", () => {
	const f = fixture();
	for (let n = 0; n < 4; n++) {
		f.hook.disconnected();
		f.enabled.dataset.serverDisabled = "true";
		f.hook.updated();
		f.hook.reconnected();
		assert.equal(f.enabled.disabled, true);
	}
	f.hook.destroyed();
	assert.equal(f.listeners.size, 0);
});

test("reconnect params carry only the open bank's bounded editable draft", () => {
	const fields = [
		{
			name: "saved_reset_policy[auto_redeem_enabled]",
			type: "hidden",
			value: "false",
		},
		{
			name: "saved_reset_policy[auto_redeem_enabled]",
			type: "checkbox",
			value: "true",
			checked: true,
		},
		{ name: "saved_reset_policy[keep_credits]", type: "number", value: "4" },
		{ name: "provider_secret", type: "text", value: "sentinel" },
	];
	const form = {
		dataset: { savedResetIdentity: "00000000-0000-4000-8000-000000000001" },
		querySelectorAll: () => fields,
	};
	const doc = {
		querySelector: (selector) =>
			selector.includes("saved-reset-policy-form") ? form : null,
	};
	assert.deepEqual(savedResetConnectParams(doc), {
		saved_reset_policy_recovery: {
			id: form.dataset.savedResetIdentity,
			policy: { auto_redeem_enabled: "true", keep_credits: "4" },
		},
	});
	assert.deepEqual(savedResetConnectParams({ querySelector: () => null }), {});
});
