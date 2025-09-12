
#!/usr/bin/env python3
"""
fetch_fb_pages.py  —  Demo de flujo para Page Public Content Access (MOCK)

Uso (terminal):
    python fetch_fb_pages.py "Base de datos diputados - Sheet1.csv" --mock

Qué hace:
  - Lee un CSV con columnas: Nombre, Partido, Pagina_publica
  - Simula (MOCK) la lectura de publicaciones públicas por Página (sin usar la API)
  - Construye un DataFrame con campos mínimos y exporta a "output_posts.csv"

Nota:
  - Esta versión NO llama a la Graph API. Sirve para grabar el screencast de revisión de Meta.
  - Cuando obtengas el permiso, reemplaza la función `fetch_public_posts_for_page` por
    llamadas reales a la Graph API con los campos requeridos.
"""
import argparse, csv, sys, json, random, hashlib
import datetime as dt
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse

def extract_page_id_or_name(url: str) -> str:
    """
    Extrae un identificador legible de la URL pública de Facebook.
    - Si es un perfil "legacy" podría contener query params; tomamos el path limpio.
    - Esto NO garantiza el page_id numérico; es un alias para demo/mock.
    """
    try:
        parsed = urlparse(url)
        # path como '/NombreDePagina/' -> 'NombreDePagina'
        slug = parsed.path.strip('/').split('/')[0]
        return slug or "unknown_page"
    except Exception:
        return "unknown_page"

def seeded_rng(seed_base:str):
    """Genera un RNG determinista por página para reproducibilidad en el screencast."""
    seed_int = int(hashlib.sha256(seed_base.encode("utf-8")).hexdigest(), 16) % (10**8)
    rnd = random.Random(seed_int)
    return rnd

def fetch_public_posts_for_page(page_name:str, page_url:str, how_many:int=3) -> list:
    """
    MOCK: Devuelve una lista de posts simulados con campos mínimos.
    Reemplaza este mock por llamadas reales a la Graph API una vez aprobado el permiso.
    """
    slug = extract_page_id_or_name(page_url)
    rnd = seeded_rng(slug or page_name)
    posts = []
    base_time = dt.datetime(2025, 9, 10, 12, 0, 0)  # fecha fija para la demo

    for i in range(how_many):
        created = base_time - dt.timedelta(hours=i * rnd.randint(3, 9))
        post_id = f"{slug}_{created.strftime('%Y%m%d%H%M%S')}"
        msg_templates = [
            "Publicación sobre reforma {topic}.",
            "Informe de trabajo en {topic}.",
            "Declaración respecto a {topic}.",
            "Resumen semanal: avances en {topic}.",
            "Sesión en el Congreso relacionada con {topic}."
        ]
        topics = ["educación", "salud", "infraestructura", "economía", "seguridad"]
        msg = rnd.choice(msg_templates).format(topic=rnd.choice(topics))

        reactions = rnd.randint(10, 300)
        comments  = rnd.randint(5, 120)
        shares    = rnd.randint(0, 40)
        posts.append({
            "page_name": page_name,
            "page_url": page_url,
            "page_id_alias": slug,
            "post_id": post_id,
            "created_time": created.strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "message": msg,
            "permalink_url": f"https://www.facebook.com/{slug}/posts/{post_id}",
            "reactions_count": reactions,
            "comments_count": comments,
            "shares_count": shares,
        })
    return posts

def main():
    parser = argparse.ArgumentParser(description="Demo flujo Page Public Content Access (MOCK)")
    parser.add_argument("input_csv", help="Ruta al CSV con columnas: Nombre, Partido, Pagina_publica")
    parser.add_argument("--mock", action="store_true", help="Usar datos simulados (sin API)")
    parser.add_argument("--per_page", type=int, default=3, help="Posts simulados por Página (default: 3)")
    parser.add_argument("--out", default="output_posts.csv", help="Archivo de salida CSV")
    args = parser.parse_args()

    input_path = Path(args.input_csv)
    if not input_path.exists():
        print(f"[ERROR] No se encontró el CSV de entrada: {input_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Leyendo lista de Páginas desde: {input_path}")
    df_pages = pd.read_csv(input_path)
    expected_cols = {"Nombre", "Partido", "Pagina_publica"}
    if not expected_cols.issubset(set(df_pages.columns)):
        print(f"[ERROR] CSV debe contener columnas {expected_cols}. Encontradas: {list(df_pages.columns)}", file=sys.stderr)
        sys.exit(2)

    all_posts = []
    for _, row in df_pages.iterrows():
        nombre = str(row["Nombre"]).strip()
        url    = str(row["Pagina_publica"]).strip()
        if not url or url.lower() == "nan":
            print(f"[WARN] Sin URL para: {nombre}; se omite.")
            continue

        print(f"[INFO] Procesando Página: {nombre}  ({url})")
        if args.mock:
            posts = fetch_public_posts_for_page(nombre, url, how_many=args.per_page)
        else:
            print("[ERROR] Modo real no implementado en la demo. Usa --mock para el screencast.", file=sys.stderr)
            sys.exit(3)

        # Imprime un ejemplo tipo "respuesta de API" (solo el primero para el video)
        if posts:
            sample = posts[0].copy()
            sample_print = {
                "id": sample["post_id"],
                "message": sample["message"],
                "created_time": sample["created_time"],
                "permalink_url": sample["permalink_url"],
                "comments_count": sample["comments_count"],
                "reactions_count": sample["reactions_count"],
                "shares_count": sample["shares_count"],
            }
            print("[MOCK API RESPONSE EJEMPLO]:")
            print(json.dumps(sample_print, ensure_ascii=False, indent=2))

        all_posts.extend(posts)

    if not all_posts:
        print("[WARN] No se generaron posts (verifica el CSV de entrada).")

    df_out = pd.DataFrame(all_posts, columns=[
        "page_name","page_url","page_id_alias","post_id","created_time",
        "message","permalink_url","reactions_count","comments_count","shares_count"
    ])

    out_path = Path(args.out)
    df_out.to_csv(out_path, index=False)
    print(f"[INFO] Exportado: {out_path.resolve()}  (filas: {len(df_out)})")
    print("[OK] Demo terminada. Este flujo corresponde al uso permitido de Page Public Content Access (contenido público de Páginas).")

if __name__ == "__main__":
    main()
