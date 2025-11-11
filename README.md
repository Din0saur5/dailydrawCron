# DailyDraw Submission Cleanup

Standalone Node 18+ cron job that purges non-premium submissions whose daily prompt date has already passed. It removes the associated objects from Cloudflare R2 and then deletes the Supabase rows (public.submissions joined with public.daily_prompts).

## Requirements

- Node.js 18 or newer
- Supabase project with `user_is_premium(user_id uuid)` RPC returning a boolean
- Cloudflare R2 bucket with API credentials (S3-compatible)

## Environment Variables

Set the following variables before running locally or configuring Render:

| Variable | Description |
| --- | --- |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role API key |
| `R2_ACCOUNT_ID` | Cloudflare account ID (used for default endpoint) |
| `R2_ACCESS_KEY_ID` | Cloudflare R2 access key |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 secret key |
| `R2_BUCKET_NAME` | Target bucket containing submission originals |
| `R2_ENDPOINT` | Optional custom S3 endpoint; defaults to `https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com` |

Create a local `.env` (Render supports the same vars) and export it before running:

```bash
export $(grep -v '^#' .env | xargs)
```

## Install & Run

```bash
npm install
npm run start
```

The start script runs the TypeScript entrypoint (`src/cleanup.ts`) via ts-node. When scheduled on Render, set the cron job command to `npm run start`.

## How It Works

1. Fetches up to 1000 submissions per batch where `daily_prompts.prompt_date < timezone('UTC', now())::date` via Supabase (inner join on `daily_prompts`).
2. Uses the `user_is_premium(user_id)` RPC to skip premium users, caching results per run.
3. Deletes the associated `original_key` objects from R2; only deletes Supabase rows if the storage purge succeeds.
4. Repeats until no more matching submissions remain, logging the number of files/rows removed per batch.

The script exits successfully (code 0) even when there is nothing to delete, and will throw if either the R2 or Supabase deletes fail.
