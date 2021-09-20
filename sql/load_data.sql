CREATE OR REPLACE TABLE `wide-link-319313.dota2_dwh.d_heroes`
AS SELECT * from `wide-link-319313.dota2_dwh_staging.d_heroes`;

CREATE OR REPLACE TABLE `wide-link-319313.dota2_dwh.d_items`
AS SELECT * from `wide-link-319313.dota2_dwh_staging.d_items`;

MERGE INTO `wide-link-319313.dota2_dwh.d_players` T
USING `wide-link-319313.dota2_dwh_staging.d_players` S
ON T.player_id = S.player_id
WHEN MATCHED THEN UPDATE SET
player_name = S.player_name
WHEN NOT MATCHED THEN
INSERT (player_id, player_name) VALUES (S.player_id, S.player_name);


MERGE INTO `wide-link-319313.dota2_dwh.d_teams` T
USING `wide-link-319313.dota2_dwh_staging.d_teams` S
ON T.team_id = S.team_id
WHEN MATCHED THEN UPDATE SET
team_name = S.team_name
WHEN NOT MATCHED THEN
INSERT (team_id, team_name) VALUES (S.team_id, S.team_name);

INSERT INTO `wide-link-319313.dota2_dwh.f_records`
SELECT * FROM `wide-link-319313.dota2_dwh_staging.f_records`;