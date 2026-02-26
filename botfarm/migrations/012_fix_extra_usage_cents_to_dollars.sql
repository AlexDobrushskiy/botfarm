-- Fix historical extra usage values: API returns cents, we stored as-is.
-- Divide by 100 to convert to dollars.
UPDATE usage_snapshots
SET extra_usage_monthly_limit = extra_usage_monthly_limit / 100.0,
    extra_usage_used_credits  = extra_usage_used_credits / 100.0
WHERE extra_usage_monthly_limit IS NOT NULL
   OR extra_usage_used_credits IS NOT NULL;
