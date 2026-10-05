"""
Polymarket ingestion: config -> WebSocket client -> message parsing ->
adapter (to the shared models.py contracts) -> feed (owns the OrderBooks).

Also: fees (taker fee per fill), recorder (raw-frame tapes), and two
command-line tools, gamma (market lookup) and inspect_tape (tape viewer).
"""
