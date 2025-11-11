"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const supabase_js_1 = require("@supabase/supabase-js");
const client_s3_1 = require("@aws-sdk/client-s3");
const BATCH_SIZE = 1000;
const PREMIUM_RPC_CHUNK_SIZE = 25;
const requiredEnvVars = [
    'SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
    'R2_ACCOUNT_ID',
    'R2_ACCESS_KEY_ID',
    'R2_SECRET_ACCESS_KEY',
    'R2_BUCKET_NAME',
];
const env = requiredEnvVars.reduce((acc, key) => {
    const value = process.env[key];
    if (!value) {
        throw new Error(`Missing required environment variable: ${key}`);
    }
    acc[key] = value;
    return acc;
}, {});
const supabase = (0, supabase_js_1.createClient)(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
        persistSession: false,
        autoRefreshToken: false,
    },
});
const r2Endpoint = process.env.R2_ENDPOINT ||
    `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
const s3 = new client_s3_1.S3Client({
    region: 'auto',
    endpoint: r2Endpoint,
    forcePathStyle: true,
    credentials: {
        accessKeyId: env.R2_ACCESS_KEY_ID,
        secretAccessKey: env.R2_SECRET_ACCESS_KEY,
    },
});
const premiumStatusCache = new Map();
async function fetchBatch(cutoffDate, lastSeenId) {
    let query = supabase
        .from('submissions')
        .select('id,user_id,original_key,daily_prompts!inner(prompt_date)')
        .lt('daily_prompts.prompt_date', cutoffDate)
        .order('id', { ascending: true })
        .limit(BATCH_SIZE);
    if (lastSeenId) {
        query = query.gt('id', lastSeenId);
    }
    const { data, error } = await query;
    if (error) {
        throw new Error(`Failed to fetch submissions batch: ${error.message ?? error}`);
    }
    return data ?? [];
}
async function fetchPremiumStatus(userId) {
    if (premiumStatusCache.has(userId)) {
        return premiumStatusCache.get(userId);
    }
    const { data, error } = await supabase.rpc('user_is_premium', {
        user_id: userId,
    });
    if (error) {
        throw new Error(`user_is_premium RPC failed for user ${userId}: ${error.message ?? error}`);
    }
    if (typeof data !== 'boolean') {
        throw new Error(`user_is_premium RPC returned non-boolean for user ${userId}`);
    }
    premiumStatusCache.set(userId, data);
    return data;
}
async function ensurePremiumStatuses(userIds) {
    const missing = userIds.filter((id) => !premiumStatusCache.has(id));
    for (let i = 0; i < missing.length; i += PREMIUM_RPC_CHUNK_SIZE) {
        const chunk = missing.slice(i, i + PREMIUM_RPC_CHUNK_SIZE);
        await Promise.all(chunk.map((id) => fetchPremiumStatus(id)));
    }
}
async function filterNonPremium(rows) {
    const uniqueUserIds = Array.from(new Set(rows.map((row) => row.user_id)));
    await ensurePremiumStatuses(uniqueUserIds);
    return rows.filter((row) => premiumStatusCache.get(row.user_id) === false);
}
async function deleteFilesFromR2(keys) {
    if (!keys.length) {
        return 0;
    }
    const uniqueKeys = Array.from(new Set(keys));
    const command = new client_s3_1.DeleteObjectsCommand({
        Bucket: env.R2_BUCKET_NAME,
        Delete: {
            Objects: uniqueKeys.map((Key) => ({ Key })),
            Quiet: true,
        },
    });
    const response = await s3.send(command);
    const errors = response.Errors ?? [];
    if (errors.length > 0) {
        const formatted = errors
            .map((err) => `${err.Key ?? 'unknown'}: ${err.Message ?? 'unspecified error'}`)
            .join('; ');
        throw new Error(`Failed to delete some objects from R2: ${formatted}`);
    }
    return response.Deleted?.length ?? uniqueKeys.length;
}
async function deleteSubmissions(ids) {
    if (!ids.length) {
        return 0;
    }
    const { error } = await supabase.from('submissions').delete().in('id', ids);
    if (error) {
        throw new Error(`Failed to delete submissions ${ids.join(', ')}: ${error.message ?? error}`);
    }
    return ids.length;
}
async function main() {
    const cutoffDate = new Date().toISOString().slice(0, 10);
    console.log(`Starting cleanup. UTC cutoff date: ${cutoffDate}`);
    let totalRowsDeleted = 0;
    let totalFilesDeleted = 0;
    let batchNumber = 1;
    let lastSeenId = null;
    while (true) {
        const batch = await fetchBatch(cutoffDate, lastSeenId);
        if (batch.length === 0) {
            break;
        }
        lastSeenId = batch[batch.length - 1]?.id ?? lastSeenId;
        const removable = await filterNonPremium(batch);
        if (!removable.length) {
            console.log(`Batch ${batchNumber}: no deletable submissions (all premium users).`);
            batchNumber += 1;
            continue;
        }
        const keys = removable
            .map((row) => row.original_key)
            .filter((key) => Boolean(key));
        console.log(`Batch ${batchNumber}: attempting to delete ${removable.length} submissions and ${keys.length} files.`);
        await deleteFilesFromR2(keys);
        await deleteSubmissions(removable.map((row) => row.id));
        console.log(`Batch ${batchNumber}: deleted ${removable.length} submissions and ${keys.length} files.`);
        totalRowsDeleted += removable.length;
        totalFilesDeleted += keys.length;
        batchNumber += 1;
    }
    console.log(`Cleanup finished. Deleted ${totalRowsDeleted} submissions and ${totalFilesDeleted} files.`);
}
main().catch((error) => {
    console.error('Cleanup failed:', error);
    process.exitCode = 1;
});
