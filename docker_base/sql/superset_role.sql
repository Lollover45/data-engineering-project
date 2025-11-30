DROP ROLE IF EXISTS role_superset; 
DROP USER IF EXISTS user_superset; 
CREATE ROLE role_superset;
CREATE USER user_superset IDENTIFIED WITH sha256_password BY 'ss_very_secret_password';
GRANT role_superset TO user_superset;
GRANT SELECT ON messud.* TO role_superset;