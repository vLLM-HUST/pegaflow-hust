"""Full-cost selective advisory trigger against its unchanged same-round parent."""

from analyze_linear_charged import main

if __name__ == "__main__":
    main(
        "advice-release-window-01",
        "advice-window-audit.json",
        "prefix-reclaim-calendar-02",
    )
