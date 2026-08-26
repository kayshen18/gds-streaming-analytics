# GDS Data Dictionary

## Source fields

The source is comma-delimited but may contain variable-length payload fields. The baseline parser relies only on the first five fields:

| Position | Normalized name | Meaning |
|---:|---|---|
| 1 | `group_id` | Log grouping identifier, such as `TB.P1780` |
| 2 | `log_type` | Supported values are `ITARES` and `ITAREQ` |
| 3 | `event_date` | Calendar date in `YYYYMMDD` form |
| 4 | `event_hour` | Integer hour from 0 through 23 |
| 5 | `event_time` | Source time text, such as `19:45:36:257` |

For valid ITARES records, success tokens are extracted with the grammar `[A-Z0-9]{2}:success`. The captured two-character code becomes `airline_code`.

## Metrics

- `successful_response_records`: number of valid ITARES source records containing at least one success token for an airline. A source line contributes at most one count per airline.
- `successful_booking_tokens`: total matched success tokens. `CA:success;CA:success;` contributes two CA tokens.

The dataset does not establish that a token equals a ticket, passenger, booking, order, or revenue event. The project therefore does not use those labels.

Primary metrics preserve every source record. Duplicate fingerprints and counts are reported separately and do not silently alter aggregation.

## Parse status and failure reasons

- `valid`: supported record with valid required fields.
- `invalid`: malformed required data.
- `unsupported`: structurally valid record whose type is outside ITARES/ITAREQ.

Stable failure reasons:

- `blank_line`
- `too_few_fields`
- `missing_group_id`
- `missing_log_type`
- `invalid_date`
- `invalid_hour`
- `unsupported_log_type`
