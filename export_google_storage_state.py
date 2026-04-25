from pathlib import Path
from playwright.sync_api import sync_playwright

SESSION_DIR = Path('./google_session')
OUT_FILE = Path('./google_storage_state.json')


def main():
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(SESSION_DIR),
            headless=False,
            slow_mo=100,
            locale='pt-BR',
            args=['--disable-blink-features=AutomationControlled'],
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto('https://accounts.google.com/', wait_until='domcontentloaded')
        print('Confirme que a conta está logada no navegador. Depois pressione ENTER para exportar o storage state...')
        input()
        context.storage_state(path=str(OUT_FILE))
        context.close()
        print(f'Storage state exportado para: {OUT_FILE.resolve()}')


if __name__ == '__main__':
    main()
