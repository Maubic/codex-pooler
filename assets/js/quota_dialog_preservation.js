const quotaDialogs = (container) => [...new Set([
	container.closest("dialog[data-quota-dialog-preserve]"),
	...container.querySelectorAll("dialog[data-quota-dialog-preserve]"),
].filter(Boolean))];

const visibleControl = (element) => {
	if (!element?.isConnected || element.hasAttribute("disabled")) return false;
	if (element.closest("[hidden], [inert], [aria-hidden='true']")) return false;
	return element.checkVisibility
		? element.checkVisibility({checkOpacity: true, checkVisibilityCSS: true})
		: element.getClientRects().length > 0;
};

// LiveSocket calls these around a complete morph. State belongs to that patch
// only: it never becomes the authority for server data or a removed dialog.
export const createQuotaDialogPreservation = () => {
	const patches = new WeakMap();

	return {
		onPatchStart(container) {
			const document = container.ownerDocument;
			patches.set(container, quotaDialogs(container)
				.filter(dialog => dialog.hasAttribute("open"))
				.map(dialog => ({
					id: dialog.id,
					focused: dialog.contains(document.activeElement) ? document.activeElement : null,
					scroll: [...dialog.querySelectorAll("[data-preserve-scroll]")].map(element => ({
						id: element.id, top: element.scrollTop, left: element.scrollLeft,
					})),
				})));
		},
		onBeforeElUpdated(from, to) {
			if (!from.hasAttribute("data-preserve-open") || !to.hasAttribute("data-preserve-open")) return;
			if (from.hasAttribute("open")) {
				to.setAttribute("open", "");
			} else if (from.closest("[data-quota-dialog-preserve]")) {
				// A user-collapsed source stays collapsed even when a candidate's
				// server default is expanded on a later refresh.
				to.removeAttribute("open");
			}
		},
		onPatchEnd(container) {
			const snapshots = patches.get(container) ?? [];
			patches.delete(container);
			const document = container.ownerDocument;
			for (const snapshot of snapshots) {
				const dialog = document.getElementById(snapshot.id);
				if (!dialog?.hasAttribute("data-quota-dialog-preserve") || !dialog.hasAttribute("open")) continue;
				if (snapshot.focused) {
					const focused = snapshot.focused.id
						? document.getElementById(snapshot.focused.id)
						: snapshot.focused;
					const target = dialog.contains(focused) && visibleControl(focused)
						? focused
						: [dialog.querySelector("[data-dialog-focus-fallback]"), dialog.querySelector("[data-role='dialog-dismiss']")].find(visibleControl);
					if (target && document.activeElement !== target) target.focus({preventScroll: true});
				}
				for (const position of snapshot.scroll) {
					const element = document.getElementById(position.id);
					if (!element || !dialog.contains(element)) continue;
					element.scrollTop = position.top;
					element.scrollLeft = position.left;
				}
			}
		},
	};
};
