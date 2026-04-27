from __future__ import annotations

import os
import shutil
from pathlib import Path

_IGNORE_NAMES = {
    'SingletonCookie',
    'SingletonLock',
    'SingletonSocket',
    'BrowserMetrics',
}
_IGNORE_PREFIXES = (
    '.com.google.Chrome.',
)
_IGNORE_DIR_NAMES = {
    'Cache',
    'Cache_Data',
    'Code Cache',
    'GPUCache',
    'DawnGraphiteCache',
    'DawnWebGPUCache',
    'GrShaderCache',
    'GraphiteDawnCache',
    'ShaderCache',
}


def is_profile_in_use(profile_dir: Path) -> bool:
    """True se há processo Chrome rodando com esse profile dir."""
    profile_str = str(profile_dir.resolve())
    try:
        for pid_path in Path("/proc").iterdir():
            if not pid_path.name.isdigit():
                continue
            try:
                cmdline = (pid_path / "cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="replace")
                if profile_str in cmdline and ("chrome" in cmdline.lower() or "chromium" in cmdline.lower()):
                    return True
            except (PermissionError, FileNotFoundError):
                continue
    except Exception:
        pass
    return False

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / 'google_session'
PROFILE_GLOB = 'google_session_*'
_PROFILE_REVISION_FILES = (
    'Local State',
    'Last Version',
    'Default/Cookies',
    'Default/Preferences',
)


def purge_chrome_singleton_artifacts(profile_dir: Path) -> None:
    for name in ('SingletonCookie', 'SingletonLock', 'SingletonSocket'):
        path = profile_dir / name
        try:
            if path.exists() or path.is_symlink():
                path.unlink()
        except FileNotFoundError:
            pass


def _profile_revision(profile_dir: Path) -> float:
    candidates = [profile_dir]
    for relative in _PROFILE_REVISION_FILES:
        candidate = profile_dir / relative
        if candidate.exists():
            candidates.append(candidate)
    revision = 0.0
    for candidate in candidates:
        try:
            revision = max(revision, candidate.stat().st_mtime)
        except FileNotFoundError:
            continue
    return revision


def worker_profile_dirs(num_workers: int | None = None) -> list[Path]:
    if num_workers is not None:
        # NOTA: Worker 1 usa o profile mestre (google_session), nao google_session_1
        # Entao escravos comecam em _2, _3, etc.
        return [BASE_DIR / f'google_session_{i}' for i in range(2, max(0, int(num_workers)) + 2)]
    return [path for path in sorted(BASE_DIR.glob(PROFILE_GLOB)) if path.is_dir() and path.name != SESSION_DIR.name]


def _ignore_chrome_runtime_artifacts(_src: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    for name in names:
        if (
            name in _IGNORE_NAMES
            or name in _IGNORE_DIR_NAMES
            or any(name.startswith(prefix) for prefix in _IGNORE_PREFIXES)
        ):
            ignored.add(name)
    return ignored


def _copy_profile_tree(src: Path, dst: Path) -> None:
    """Copia best-effort de perfil, tolerando arquivos efêmeros sumindo e erros de permissão."""
    try:
        shutil.copytree(
            src,
            dst,
            dirs_exist_ok=True,
            ignore=_ignore_chrome_runtime_artifacts,
            ignore_dangling_symlinks=True,
        )
    except shutil.Error as exc:
        filtered_errors = []
        for entry in exc.args[0] if exc.args else []:
            try:
                _src, _dst, msg = entry
            except Exception:
                filtered_errors.append(entry)
                continue
            lowered = str(msg).lower()
            if 'no such file or directory' in lowered:
                continue
            if 'permission denied' in lowered or 'operation not permitted' in lowered:
                continue
            filtered_errors.append(entry)
        if filtered_errors:
            raise shutil.Error(filtered_errors)


def sync_base_session_to_worker_profiles(num_workers: int | None = None, force: bool = False, skip_in_use: bool = False) -> list[Path]:
    if not SESSION_DIR.is_dir():
        return []

    source_revision = _profile_revision(SESSION_DIR)
    copied: list[Path] = []
    for profile_dir in worker_profile_dirs(num_workers=num_workers):
        profile_dir.mkdir(parents=True, exist_ok=True)
        if skip_in_use and is_profile_in_use(profile_dir):
            continue
        target_revision = _profile_revision(profile_dir)
        if not force and target_revision >= source_revision:
            purge_chrome_singleton_artifacts(profile_dir)
            continue
        _copy_profile_tree(SESSION_DIR, profile_dir)
        purge_chrome_singleton_artifacts(profile_dir)
        copied.append(profile_dir)
    return copied


def sync_current_worker_profile_from_base() -> bool:
    current_profile = Path(os.getenv('GOOGLE_PERSISTENT_PROFILE_DIR', '')).resolve() if os.getenv('GOOGLE_PERSISTENT_PROFILE_DIR') else None
    if not current_profile or current_profile == SESSION_DIR.resolve():
        return False
    if not SESSION_DIR.is_dir():
        return False

    current_profile.mkdir(parents=True, exist_ok=True)
    source_revision = _profile_revision(SESSION_DIR)
    target_revision = _profile_revision(current_profile)
    if target_revision >= source_revision:
        purge_chrome_singleton_artifacts(current_profile)
        return False

    _copy_profile_tree(SESSION_DIR, current_profile)
    purge_chrome_singleton_artifacts(current_profile)
    return True
