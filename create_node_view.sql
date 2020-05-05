CREATE VIEW `nodes` AS
    SELECT 
        `user_details`.`id` AS `id`,
        `user_details`.`status_lang` AS `status_lang`,
        `user_details`.`screen_name` AS `screen_name`,
        `user_details`.`created_at` AS `created_at`,
        `user_details`.`favourites_count` AS `favourites_count`,
        `user_details`.`followers_count` AS `followers_count`,
        `user_details`.`friends_count` AS `friends_count`,
        `user_details`.`listed_count` AS `listed_count`,
        `user_details`.`protected` AS `protected`,
        `user_details`.`statuses_count` AS `statuses_count`,
        `user_details`.`status_created_at` AS `status_created_at`,
        `user_details`.`timestamp` AS `timestamp`,
        `user_details`.`verified` AS `verified`
    FROM
        `user_details`
    WHERE
        `user_details`.`id` IN (SELECT 
                `T`.`id`
            FROM
                (SELECT 
                    `result`.`source` AS `id`
                FROM
                    `result` UNION SELECT 
                    `result`.`target` AS `id`
                FROM
                    `result`) `T`)