"""Full-cost optional advisory ablation, compared only to its same-round parent."""

from analyze_linear_charged import main

if __name__ == "__main__":
    main("no-decode-advice-01", "no-decode-advice-audit.json", "prefix-reclaim-calendar-02")
