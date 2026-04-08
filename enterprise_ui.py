import os

import uvicorn

from barca_control_center import enterprise_ui as _impl

globals().update({name: getattr(_impl, name) for name in dir(_impl) if not name.startswith("__")})


def main() -> None:
    port = int(os.getenv("BARCA_UI_PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
