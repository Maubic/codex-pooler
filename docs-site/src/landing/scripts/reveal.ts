// Fade sections in as they scroll into view. Without IntersectionObserver
// (or with reduced motion) everything is simply shown.
const items = Array.from(document.querySelectorAll<HTMLElement>("[data-reveal]"));
const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

if (!("IntersectionObserver" in window) || reduceMotion) {
  items.forEach((item) => item.classList.add("is-in"));
} else {
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        entry.target.classList.add("is-in");
        observer.unobserve(entry.target);
      }
    },
    { rootMargin: "0px 0px -8% 0px", threshold: 0.12 },
  );
  items.forEach((item) => observer.observe(item));
}
