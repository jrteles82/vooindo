#!/usr/bin/env python3
"""Tenta login com display virtual e debug screenshots."""
import sys, os, time, json

os.chdir('/opt/vooindo')
sys.path.insert(0, '/opt/vooindo')

from playwright.sync_api import sync_playwright

email = 'vooindo.bot@gmail.com'
password = 'Vooindo#8212'
code_2fa = sys.argv[1] if len(sys.argv) > 1 else None

dump_dir = '/opt/vooindo/debug_dumps'
os.makedirs(dump_dir, exist_ok=True)

profile = '/opt/vooindo/google_session'

# Limpar sessão
import shutil
if os.path.exists(profile):
    shutil.rmtree(profile)
for l in ['google_session.lock', 'google_session_3.lock']:
    lpath = os.path.join('/opt/vooindo', l)
    if os.path.exists(lpath):
        os.remove(lpath)

with sync_playwright() as p:
    browser = p.chromium.launch_persistent_context(
        user_data_dir=profile,
        headless=True,
        args=[
            '--no-sandbox', '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1280,720',
        ],
        locale='pt-BR',
        timezone_id='America/Porto_Velho',
        no_viewport=True,
    )
    
    page = browser.pages[0] if browser.pages else browser.new_page()
    page.goto('https://accounts.google.com/signin', timeout=30000)
    page.wait_for_timeout(2000)
    
    # Preencher email
    email_input = page.locator('input[type="email"]')
    if email_input.count() > 0:
        email_input.fill(email)
        page.keyboard.press('Enter')
        page.wait_for_timeout(3000)
        page.screenshot(path=os.path.join(dump_dir, '1_after_email.png'))
    
    # Preencher senha
    pass_input = page.locator('input[type="password"]')
    if pass_input.count() > 0:
        pass_input.fill(password)
        page.keyboard.press('Enter')
        page.wait_for_timeout(3000)
        page.screenshot(path=os.path.join(dump_dir, '2_after_password.png'))
    
    # Check se precisa de 2FA
    for i in range(15):
        page.wait_for_timeout(2000)
        url = page.url
        body = page.content()
        page.screenshot(path=os.path.join(dump_dir, f'3_step_{i:02d}.png'))
        
        print(f"Passo {i}: url={url[:80]}")
        
        # Verificar se está no myaccount (login OK)
        if 'myaccount.google.com' in url:
            print("✅ Login OK! Redirecionado para myaccount")
            break
        
        # Verificar 2FA
        if any(k in body.lower() for k in ['verificação', 'código', 'autenticador', 'totp', '2-step', 'confirme seu telefone']):
            print("🔴 NEED 2FA detectado!")
            if code_2fa:
                code_input = page.locator('input[type="tel"], input[name="totpPin"], input[type="number"]')
                if code_input.count() > 0:
                    code_input.first.fill(code_2fa)
                else:
                    page.keyboard.type(code_2fa)
                page.wait_for_timeout(500)
                page.keyboard.press('Enter')
                page.wait_for_timeout(3000)
                page.screenshot(path=os.path.join(dump_dir, '4_after_2fa.png'))
                print(f"Código {code_2fa} enviado")
            else:
                print("⚠️ Código 2FA necessário mas não fornecido")
                break
            continue
        
        # Clicar "Avançar"/"Continuar"
        btn = page.locator('button:has-text("Avançar"), button:has-text("Continuar"), button:has-text("Next"), button:has-text("Continue")').first
        if btn.count() > 0 and btn.is_visible():
            btn.click()
            page.wait_for_timeout(1000)
    
    # Verificar estado final
    final_url = page.url
    final_body = page.content()
    print(f"\nURL final: {final_url}")
    
    if 'myaccount' in final_url:
        print("✅ Sessão autenticada!")
    else:
        print("❌ Sessão NÃO autenticada")
    
    browser.close()
