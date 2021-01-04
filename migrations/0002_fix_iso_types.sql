alter table "public"."languages" alter column "iso3166" set data type text using "iso3166"::text;

alter table "public"."languages" alter column "iso639" set data type text using "iso639"::text;

