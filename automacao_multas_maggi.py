# ===============================================================
# AUTOMAÇÃO RH - MULTAS MAGGI (2 PLANILHAS DEFINITIVAS)
# ===============================================================

import imaplib
import email
import smtplib
import gspread
import pandas as pd
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import decode_header
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import traceback

# ===============================================================
# CONFIGURAÇÕES
# ===============================================================

# -------- EMAIL --------
IMAP_SERVER = "imap.zoho.com"
IMAP_PORT = 993
SMTP_SERVER = "smtp.zoho.com"
SMTP_PORT = 587
EMAIL_USER = "servicos@smart.log.br"
EMAIL_PASS = "KxwZjXuNAcZc"
PASTA_PROCESSADOS = "INBOX.Processed_RH"
DESTINO_RESUMO = "sidenei.silva@smart.log.br"

# -------- GOOGLE --------

CREDENCIAIS_JSON = r"C:\Users\Sidenei Silva\Desktop\PROJETO_PYTHON\AUTOMACAO_RH_PLAN_MULTAS\CredenciaisJSON\credenciais.json"

# URLs (somente até o ID, sem /edit ou #gid)
URL_PLANILHA_MULTAS = "https://docs.google.com/spreadsheets/d/1FWmDdPl6_Fa9hsusHrYVhG1prV9IIywIKSUQiTBHh5g"

URL_PLANILHA_BASE = "https://docs.google.com/spreadsheets/d/1-BoeN9ZzlW2kkxZh9J2138ZwPWbhcakvWPie-XFOfWo"

# Abas
ABA_MULTAS = "MULTAS"
ABA_EMBARCADOR = "Tela Embarcador"
ABA_MOTORISTAS = "Motoristas"

VALOR_FIXO_TOTAL = 390.46

# ===============================================================
# CONEXÃO GOOGLE
# ===============================================================

def conectar_google():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENCIAIS_JSON,
        scope
    )

    client = gspread.authorize(creds)

    planilha_multas = client.open_by_url(URL_PLANILHA_MULTAS)
    planilha_base = client.open_by_url(URL_PLANILHA_BASE)

    return planilha_multas, planilha_base



# ===============================================================
# FUNÇÕES AUXILIARES
# ===============================================================

def conectar_google():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENCIAIS_JSON,
        scope
    )

    client = gspread.authorize(creds)

    planilha_multas = client.open_by_url(URL_PLANILHA_MULTAS)
    planilha_base = client.open_by_url(URL_PLANILHA_BASE)

    return planilha_multas, planilha_base


def conectar_email():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select("inbox")
    return mail


def extrair_html(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                return part.get_payload(decode=True).decode(errors="ignore")
    return None


def extrair_tabela(html):
    soup = BeautifulSoup(html, "html.parser")
    tabela = soup.find("table")
    if not tabela:
        return None

    linhas = []
    for tr in tabela.find_all("tr"):
        cols = tr.find_all(["td", "th"])
        linhas.append([c.get_text(strip=True) for c in cols])
    return linhas


def normalizar_colunas(headers):
    return [h.strip().upper() for h in headers]


def enviar_email_resumo(lancados):
    if not lancados:
        return

    # Corpo do e-mail com tabela
    corpo = """
    <h3>Multas lançadas automaticamente:</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse;">
        <thead>
            <tr style="background-color:#f2f2f2;">
                <th>AIT</th>
                <th>PLACA</th>
            </tr>
        </thead>
        <tbody>
    """

    for item in lancados:
        ait, placa = item.split(" - ")
        corpo += f"""
            <tr>
                <td>{ait}</td>
                <td>{placa}</td>
            </tr>
        """

    corpo += """
        </tbody>
    </table>
    """

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = DESTINO_RESUMO
    msg["Subject"] = "Resumo - Multas Lançadas"

    msg.attach(MIMEText(corpo, "html"))

    smtp = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    smtp.starttls()
    smtp.login(EMAIL_USER, EMAIL_PASS)
    smtp.sendmail(EMAIL_USER, DESTINO_RESUMO, msg.as_string())
    smtp.quit()


def mover_email(mail, uid):
    mail.uid("COPY", uid, PASTA_PROCESSADOS)
    mail.uid("STORE", uid, "+FLAGS", r"(\Deleted)")
    mail.expunge()

# ===============================================================
# PROCESSAMENTO PRINCIPAL
# ===============================================================

print("🚀 Iniciando automação MAGGI...")

try:

    # GOOGLE
    planilha_multas, planilha_base = conectar_google()

    aba_multas = planilha_multas.worksheet(ABA_MULTAS)
    aba_embarcador = planilha_base.worksheet(ABA_EMBARCADOR)
    aba_motoristas = planilha_base.worksheet(ABA_MOTORISTAS)

    df_multas = pd.DataFrame(aba_multas.get_all_records())
    aits_existentes = df_multas["AIT"].astype(str).tolist() if not df_multas.empty else []

    df_embarcador = pd.DataFrame(aba_embarcador.get_all_records())
    df_motoristas = pd.DataFrame(aba_motoristas.get_all_records())

    # Normalizações
    df_embarcador["placa"] = df_embarcador["placa"].astype(str).str.upper().str.strip()
    df_embarcador["data_expedicao"] = pd.to_datetime(
        df_embarcador["data_expedicao"],
        dayfirst=True,
        errors="coerce"
    )

    df_motoristas["cpf_motorista"] = df_motoristas["cpf_motorista"].astype(str).str.strip()

    # EMAIL
    mail = conectar_email()
    result, data = mail.uid("search", None, "ALL")
    uids = data[0].split()

    lancados_resumo = []

    for uid in uids:

        result, msg_data = mail.uid("fetch", uid, "(RFC822)")
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        subject, encoding = decode_header(msg["Subject"])[0]
        if isinstance(subject, bytes):
            subject = subject.decode(encoding if encoding else "utf-8")

        if "transito" not in subject.lower():
            continue

        html = extrair_html(msg)
        if not html:
            continue

        tabela = extrair_tabela(html)
        if not tabela or len(tabela) <= 1:
            continue

        headers = normalizar_colunas(tabela[0])
        dados = tabela[1:]

        for linha in dados:

            registro = dict(zip(headers, linha))

            # 🔒 Captura robusta das colunas
            ait = ""
            placa = ""
            data_infracao = ""

            for chave, valor in registro.items():
                chave_upper = chave.upper()

                if "AIT" in chave_upper:
                    ait = str(valor).strip()

                if "PLACA" in chave_upper:
                    placa = str(valor).strip().upper()

                if "INFRA" in chave_upper:
                    data_infracao = str(valor).strip()

            if not ait or ait in aits_existentes:
                continue

            data_infracao_dt = pd.to_datetime(
                data_infracao,
                dayfirst=True,
                errors="coerce"
            )

            motorista = ""
            empresa = ""
            unidade = ""
            viagem = "NÃO LOCALIZADO"
            status_ativo = "NÃO LOCALIZADO"

            if placa and pd.notnull(data_infracao_dt):

                filtro = df_embarcador[
                    (df_embarcador["placa"] == placa)
                    &
                    (df_embarcador["data_expedicao"].dt.date == data_infracao_dt.date())
                ]

                if not filtro.empty:
                    registro_base = filtro.iloc[0]

                    motorista = str(registro_base.get("nome_motorista", "")).strip().upper()
                    empresa = registro_base.get("agencia", "")
                    unidade = registro_base.get("operacao_carregamento", "")
                    viagem = registro_base.get("viagem", "")
                    cpf = str(registro_base.get("cpf_motorista", "")).strip()

                    validacao = df_motoristas[
                        df_motoristas["cpf_motorista"] == cpf
                    ]

                    if not validacao.empty:
                        status_ativo = (
                            "ATIVO"
                            if str(validacao.iloc[0].get("ativo", "")).upper() == "SIM"
                            else "INATIVO"
                        )

            nova_linha = [
                "MAGGI",
                datetime.now().strftime("%d/%m/%Y"),
                ait,
                placa,
                data_infracao_dt.strftime("%d/%m/%Y") if pd.notnull(data_infracao_dt) else "",
                motorista,
                status_ativo,
                empresa,
                unidade,
                f"'R$ {VALOR_FIXO_TOTAL:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                viagem,
            ]

            aba_multas.append_row(nova_linha, value_input_option="USER_ENTERED")
            lancados_resumo.append(f"{ait} - {placa}")
            aits_existentes.append(ait)

        mover_email(mail, uid)

    enviar_email_resumo(lancados_resumo)
    mail.logout()

    print("✅ Processo concluído com sucesso.")

except Exception:
    print("❌ ERRO:")
    print(traceback.format_exc())