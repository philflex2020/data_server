#!/usr/bin/env python3
"""
add_config.py — add a config file into web_client_status.db

Run this on any machine that has the db file and the config file.

Usage:
  python3 add_config.py "Client Name" <type> <config_file>
  python3 add_config.py "Client Name" <type> -          # read from stdin / paste

Config types (use any string, suggestions):
  telegraf   writer   influxdb   mosquitto   k8s   traefik   env

Examples:
  python3 add_config.py "Evelyn"   telegraf  /etc/telegraf/telegraf.conf
  python3 add_config.py "55 Drive" writer    /etc/writer/config.yaml
  python3 add_config.py "Anole"    influxdb  -        # then paste, Ctrl-D when done

List what is already stored:
  python3 add_config.py --list
"""
import sys, os, sqlite3, datetime, argparse, socket

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'web_client_status.db')

def do_list(conn):
    rows = conn.execute("""
        SELECT id, client_name, config_type,
               length(config_content) as chars,
               retrieved_at, source, notes
        FROM configs ORDER BY client_name, config_type
    """).fetchall()
    if not rows:
        print("No configs stored yet.")
        return
    print(f"{'ID':>4}  {'Client':<30}  {'Type':<12}  {'Chars':>6}  {'Retrieved':<20}  Notes")
    print("-" * 95)
    for r in rows:
        print(f"{r[0]:>4}  {(r[1] or ''):<30}  {(r[2] or ''):<12}  {(r[3] or 0):>6}  {(r[4] or '')[:19]:<20}  {r[6] or ''}")

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('client_name', nargs='?')
    p.add_argument('config_type', nargs='?')
    p.add_argument('file',        nargs='?', help='config file path, or - for stdin')
    p.add_argument('--notes',  default='')
    p.add_argument('--source', default=socket.gethostname())
    p.add_argument('--list',   action='store_true', help='list stored configs and exit')
    p.add_argument('--db',     default=DB, help=f'db path (default: {DB})')
    args = p.parse_args()

    conn = sqlite3.connect(args.db)

    if args.list:
        do_list(conn)
        return

    if not args.client_name or not args.config_type or not args.file:
        p.print_help()
        sys.exit(1)

    if args.file == '-':
        print(f"Paste config for '{args.client_name}' / {args.config_type}, then Ctrl-D:")
        content = sys.stdin.read()
    else:
        with open(args.file) as f:
            content = f.read()

    conn.execute("""
        INSERT INTO configs (client_name, config_type, config_content, retrieved_at, source, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (args.client_name, args.config_type, content,
          datetime.datetime.utcnow().isoformat(), args.source, args.notes))
    conn.commit()

    cur = conn.execute("SELECT id FROM configs ORDER BY id DESC LIMIT 1")
    print(f"Stored: id={cur.fetchone()[0]}  client='{args.client_name}'  "
          f"type='{args.config_type}'  {len(content):,} chars  source={args.source}")
    conn.close()

if __name__ == '__main__':
    main()
