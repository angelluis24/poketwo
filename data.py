import textwrap
from contextlib import contextmanager
from csv import DictReader
from pathlib import Path

import asyncpg
from alive_progress import alive_bar

ULTRA_BEASTS = (793, 794, 795, 796, 797, 798, 799, 803, 804, 805, 806)

csv_path = Path(__file__).parent / "pokedex" / "pokedex" / "data" / "csv"


def is_int(v):
    try:
        int(v)
    except ValueError:
        return False
    else:
        return True


def get_bool(d, key):
    return bool(d.get(key, 0))


def get_str(d, key):
    s = d.get(key)
    if s is None:
        return None
    else:
        return str(s)


def insert(obj, table, pk_cols=("id",)):
    cols, vals = zip(*obj.items())
    stmt = f"""
        INSERT INTO {table} ({", ".join(cols)})
            VALUES ({", ".join(f"${i + 1}" for i, _ in enumerate(cols))})
    """
    if pk_cols is not None and len(pk_cols) > 0:
        update_cols = [x for x in cols if x not in pk_cols]
        if len(update_cols) == 0:
            stmt += "ON CONFLICT DO NOTHING"
        if len(update_cols) == 1:
            col = update_cols[0]
            stmt += f"""
                ON CONFLICT ({", ".join(pk_cols)})
                    DO UPDATE SET
                        {col} = EXCLUDED.{col}
            """
        elif len(update_cols) > 1:
            stmt += f"""
                ON CONFLICT ({", ".join(pk_cols)})
                    DO UPDATE SET
                        ({", ".join(update_cols)}) = ({f", ".join(f"EXCLUDED.{x}" for x in update_cols)});
            """
    return stmt, *vals


def get_data_from(filename):
    with open(csv_path / filename) as f:
        rows = list(DictReader(f))
    with alive_bar(len(rows), title=filename, title_length=32) as bar:
        for row in rows:
            yield {k: int(v) if is_int(v) else v for k, v in row.items() if len(v) > 0}
            bar()


async def load_languages(conn):
    for row in get_data_from("languages.csv"):
        data = {
            "id": row["id"],
            "iso639": row["iso639"],
            "iso3166": row["iso3166"],
            "identifier": row["identifier"],
            "official": get_bool(row, "official"),
        }
        await conn.execute(*insert(data, "languages"))


async def load_pokemon_species(conn):
    for row in get_data_from("pokemon_species.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
            "is_legendary": get_bool(row, "is_legendary"),
            "is_mythical": get_bool(row, "is_mythical"),
            "is_ultra_beast": row["id"] in ULTRA_BEASTS,
        }
        await conn.execute(*insert(data, "pokemon_species"))


async def load_pokemon_species_names(conn):
    for row in get_data_from("pokemon_species_names.csv"):
        data = {
            "species_id": row["pokemon_species_id"],
            "language_id": row["local_language_id"],
            "name": row["name"],
            "genus": row.get("genus"),
        }
        await conn.execute(
            *insert(
                data, "pokemon_species_names", pk_cols=("species_id", "language_id")
            )
        )


async def load_pokemon_species_flavor_text(conn):
    for row in get_data_from("pokemon_species_flavor_text.csv"):
        data = {
            "species_id": row["species_id"],
            "language_id": row["language_id"],
            "flavor_text": row["flavor_text"],
        }
        await conn.execute(
            *insert(
                data,
                "pokemon_species_flavor_text",
                pk_cols=("species_id", "language_id"),
            )
        )


async def load_pokemon(conn):
    for row in get_data_from("pokemon.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
            "species_id": row["species_id"],
            "height": row["height"],
            "weight": row["weight"],
            "base_experience": row["base_experience"],
            "is_default": get_bool(row, "is_default"),
        }
        await conn.execute(*insert(data, "pokemon"))


async def load_evolution_triggers(conn):
    for row in get_data_from("evolution_triggers.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
        }
        await conn.execute(*insert(data, "evolution_triggers"))


async def load_items(conn):
    for row in get_data_from("items.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
            "cost": row["cost"],
        }
        await conn.execute(*insert(data, "items"))


async def load_move_targets(conn):
    for row in get_data_from("move_targets.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
        }
        await conn.execute(*insert(data, "move_targets"))


async def load_damage_classes(conn):
    for row in get_data_from("move_damage_classes.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
        }
        await conn.execute(*insert(data, "damage_classes"))


async def load_types(conn):
    for row in get_data_from("types.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
        }
        await conn.execute(*insert(data, "types"))


async def load_move_effects(conn):
    for row in get_data_from("move_effects.csv"):
        data = {"id": row["id"]}
        await conn.execute(*insert(data, "move_effects"))


async def load_moves(conn):
    for row in get_data_from("moves.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
            "type_id": row["type_id"],
            "power": row.get("power"),
            "pp": row.get("pp"),
            "accuracy": row.get("accuracy"),
            "priority": row["priority"],
            "target_id": row["target_id"],
            "damage_class_id": row.get("damage_class_id", 2),
            "effect_id": row.get("effect_id", 1),
            "effect_chance": row.get("effect_chance"),
        }
        await conn.execute(*insert(data, "moves"))


async def load_pokemon_evolutions(conn):
    for row in get_data_from("pokemon_evolution.csv"):
        data = {
            "id": row["id"],
            "evolved_species_id": row["evolved_species_id"],
            "evolution_trigger_id": row["evolution_trigger_id"],
            "trigger_item_id": row.get("trigger_item_id"),
            "minimum_level": row.get("minimum_level"),
            "held_item_id": row.get("held_item_id"),
            "time_of_day": row.get("time_of_day"),
            "known_move_id": row.get("known_move_id"),
            "known_move_type_id": row.get("known_move_type_id"),
            "minimum_happiness": row.get("minimum_happiness"),
        }
        await conn.execute(*insert(data, "pokemon_evolutions"))


async def load_pokemon_types(conn):
    for row in get_data_from("pokemon_types.csv"):
        data = {
            "pokemon_id": row["pokemon_id"],
            "type_id": row["type_id"],
        }
        await conn.execute(
            *insert(data, "pokemon_types", pk_cols=("pokemon_id", "type_id"))
        )


async def load_pokemon_forms(conn):
    for row in get_data_from("pokemon_forms.csv"):
        data = {
            "id": row["id"],
            "identifier": row["identifier"],
            "form_identifier": get_str(row, "form_identifier"),
            "pokemon_id": row["pokemon_id"],
            "is_default": get_bool(row, "is_default"),
            "is_mega": get_bool(row, "is_mega"),
            "enabled": False,
            "allow_catch": False,
            "allow_redeem": False,
        }
        await conn.execute(*insert(data, "pokemon_forms"))


async def load_pokemon_form_names(conn):
    for row in get_data_from("pokemon_form_names.csv"):
        if "form_name" not in row and "pokemon_name" not in row:
            continue
        data = {
            "form_id": row["pokemon_form_id"],
            "language_id": row["local_language_id"],
            "form_name": row.get("form_name"),
            "pokemon_name": row.get("pokemon_name"),
        }
        await conn.execute(
            *insert(data, "pokemon_form_names", pk_cols=("form_id", "language_id"))
        )


async def load_db(uri):
    conn = await asyncpg.connect(uri)
    await load_languages(conn)
    await load_pokemon_species(conn)
    await load_pokemon_species_names(conn)
    await load_pokemon_species_flavor_text(conn)
    await load_pokemon(conn)
    await load_evolution_triggers(conn)
    await load_items(conn)
    await load_move_targets(conn)
    await load_damage_classes(conn)
    await load_types(conn)
    await load_move_effects(conn)
    await load_moves(conn)
    await load_pokemon_evolutions(conn)
    await load_pokemon_types(conn)
    await load_pokemon_forms(conn)
    await load_pokemon_form_names(conn)
