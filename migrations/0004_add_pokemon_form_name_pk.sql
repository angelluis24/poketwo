CREATE UNIQUE INDEX pokemon_form_names_pkey ON public.pokemon_form_names USING btree (form_id, language_id);

alter table "public"."pokemon_form_names" add constraint "pokemon_form_names_pkey" PRIMARY KEY using index "pokemon_form_names_pkey";

