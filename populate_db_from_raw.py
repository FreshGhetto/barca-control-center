from barca_control_center import populate_db_from_raw as _impl

globals().update({name: getattr(_impl, name) for name in dir(_impl) if not name.startswith("__")})


if __name__ == "__main__":
    raise SystemExit(_impl.main())
