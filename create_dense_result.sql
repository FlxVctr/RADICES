CREATE VIEW `dense_result` AS
SELECT DISTINCT source, target FROM friends WHERE source IN
(SELECT DISTINCT T.id
            FROM
                (SELECT 
                    result.source AS id
                FROM
                    result UNION SELECT 
                    result.target AS id
                FROM
                    result) T)
AND target IN
(SELECT 
                T.id
            FROM
                (SELECT 
                    result.source AS id
                FROM
                    result UNION SELECT 
                    result.target AS id
                FROM
                    result) T)