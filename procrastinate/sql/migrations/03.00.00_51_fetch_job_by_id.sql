CREATE FUNCTION procrastinate_fetch_job_by_id_v1(
    job_id bigint
)
    RETURNS procrastinate_jobs
    LANGUAGE plpgsql
AS $$
DECLARE
    found_job procrastinate_jobs;
BEGIN
    WITH candidate AS (
        SELECT jobs.*
        FROM procrastinate_jobs AS jobs
        WHERE
            -- reject the job if its lock has earlier jobs
            NOT EXISTS (
                SELECT 1
                FROM procrastinate_jobs AS earlier_jobs
                WHERE
                    jobs.lock IS NOT NULL
                    AND earlier_jobs.lock = jobs.lock
                    AND earlier_jobs.status IN ('todo', 'doing')
                    AND earlier_jobs.id < jobs.id
            )
            AND jobs.status = 'todo'
            AND jobs.id = job_id
            AND (jobs.scheduled_at IS NULL OR jobs.scheduled_at <= now())
        ORDER BY jobs.priority DESC, jobs.id ASC
        LIMIT 1
        FOR UPDATE OF jobs SKIP LOCKED
    )
    UPDATE procrastinate_jobs
        SET status = 'doing'
        FROM candidate
        WHERE procrastinate_jobs.id = candidate.id
        RETURNING procrastinate_jobs.* INTO found_job;

    RETURN found_job;
END;
$$;
