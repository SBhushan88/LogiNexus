from collections import defaultdict, deque
from datetime import datetime, timedelta


class TumblingWindow:
    """
    Fixed-size, non-overlapping time window.
    Example: 60 seconds.
    """

    def __init__(self, window_seconds=60):
        self.window_seconds = window_seconds
        self.events = defaultdict(list)

    def add_event(self, event):
        timestamp = datetime.fromisoformat(
            event["timestamp"].replace("Z", "+00:00")
        )

        window_start = timestamp.replace(
            second=(timestamp.second // self.window_seconds)
            * self.window_seconds,
            microsecond=0
        )

        key = (event.get("city", "UNKNOWN"), window_start)

        self.events[key].append(event)

    def get_metrics(self):
        results = []

        for (city, window_start), events in self.events.items():

            speeds = [
                e.get("speed_kmph", 0)
                for e in events
                if "speed_kmph" in e
            ]

            idle_events = [
                e for e in events
                if e.get("status") == "IDLE"
            ]

            delayed_shipments = [
                e for e in events
                if e.get("status") == "DELAYED"
            ]

            avg_speed = (
                sum(speeds) / len(speeds)
                if speeds else 0
            )

            results.append({
                "window_type": "TUMBLING",
                "window_seconds": self.window_seconds,
                "window_start": window_start.isoformat(),
                "window_end": (
                    window_start +
                    timedelta(seconds=self.window_seconds)
                ).isoformat(),
                "city": city,
                "event_count": len(events),
                "avg_speed_kmph": round(avg_speed, 2),
                "idle_event_count": len(idle_events),
                "delayed_event_count": len(delayed_shipments)
            })

        return results


class SlidingWindow:
    """
    Fixed-size overlapping window.
    Example: last 3 minutes.
    """

    def __init__(self, window_seconds=180):
        self.window_seconds = window_seconds
        self.events = deque()

    def add_event(self, event):

        timestamp = datetime.fromisoformat(
            event["timestamp"].replace("Z", "+00:00")
        )

        self.events.append((timestamp, event))

        cutoff = timestamp - timedelta(
            seconds=self.window_seconds
        )

        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def get_metrics(self):

        if not self.events:
            return {}

        events = [
            event for _, event in self.events
        ]

        speeds = [
            e.get("speed_kmph", 0)
            for e in events
            if "speed_kmph" in e
        ]

        avg_speed = (
            sum(speeds) / len(speeds)
            if speeds else 0
        )

        idle_events = sum(
            1 for e in events
            if e.get("status") == "IDLE"
        )

        delayed_events = sum(
            1 for e in events
            if e.get("status") == "DELAYED"
        )

        return {
            "window_type": "SLIDING",
            "window_seconds": self.window_seconds,
            "event_count": len(events),
            "avg_speed_kmph": round(avg_speed, 2),
            "idle_event_count": idle_events,
            "delayed_event_count": delayed_events
        }