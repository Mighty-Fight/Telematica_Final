#Importaciones de librerias y dependencias necesarias
import boto3
import pymysql
from fpdf import FPDF
from datetime import datetime, timedelta
import pytz

# Configuración de la base de datos y S3
RDS_HOST = 'milodatabase.cdiikyiuqv0u.us-east-1.rds.amazonaws.com'
RDS_USER = 'Milo'
RDS_PASSWORD = 'estrella9juju'
RDS_DB = 'telematica'
S3_BUCKET = 'litmilobucket'
FILE_PREFIX = 'reporte_pagos'

#Configuracio del aspecto visual del PDF
class PDF(FPDF):
    def header(self):
        # Agregar el logo con fondo claro simulando transparencia
        self.set_fill_color(200, 200, 200)  # Color gris claro
        self.rect(0, 0, self.w, 20, 'F')  # Dibujar un rectángulo como fondo
        self.image('logo.png', 10, 5, 30)  # Logo más pequeño
        self.set_font('Times', 'B', 10)
        self.set_y(15)  # Mover el texto más abajo
        self.cell(0, 10, 'LA DOBLE AA CONTADURIA', 0, 1, 'C')
        self.ln(5)  # Espaciado reducido después del header

def lambda_handler(event, context):
    # Configurar la zona horaria local (ejemplo: América/Bogotá)
    zona_horaria = pytz.timezone("America/Bogota")
    
    # Calcular el día anterior en formato MM/DD/YYYY
    ahora_local = datetime.now(zona_horaria)
    ayer_local = (ahora_local - timedelta(days=1)).strftime('%m/%d/%Y')
    
    # Conexión a la base de datos RDS
    connection = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB
    )
    
    try:
        cursor = connection.cursor()
        
        # Consulta SQL para obtener pagos del día anterior con Empresa
        query = """
        SELECT Nombre, Apellido, Cedula, Empresa, DATE_FORMAT(FechaConsignacion, '%%m/%%d/%%Y') AS Fecha, ValorPagado
        FROM pagos
        WHERE FechaConsignacion = STR_TO_DATE(%s, '%%m/%%d/%%Y')
        """
        cursor.execute(query, (ayer_local,))
        resultados = cursor.fetchall()
        
        # Crear el reporte en PDF (orientación horizontal)
        pdf = PDF('L', 'mm', 'A4')
        pdf.add_page()
        pdf.set_font('Times', '', 10)  # Fuente más pequeña
        
        # Título del reporte
        pdf.cell(0, 10, f'Reporte de Pagos - {ayer_local}', ln=True, align='C')
        pdf.ln(5)  # Espaciado reducido
        
        # Columnas del reporte (centradas)
        pdf.set_x((pdf.w - 250) / 2)  # Centrar la tabla
        pdf.set_font('Times', 'B', 10)
        pdf.cell(40, 7, 'Nombre', 1, 0, 'C')
        pdf.cell(40, 7, 'Apellido', 1, 0, 'C')
        pdf.cell(30, 7, 'Cédula', 1, 0, 'C')
        pdf.cell(50, 7, 'Empresa', 1, 0, 'C')
        pdf.cell(40, 7, 'Fecha', 1, 0, 'C')
        pdf.cell(40, 7, 'Valor Pagado', 1, 1, 'C')
        
        # Datos de los pagos
        pdf.set_font('Times', '', 10)
        total_pagos = 0
        total_valor = 0
        
        for row in resultados:
            nombre, apellido, cedula, empresa, fecha, valor_pagado = row
            pdf.set_x((pdf.w - 250) / 2)  # Centrar la tabla
            pdf.cell(40, 7, nombre, 1, 0, 'C')
            pdf.cell(40, 7, apellido, 1, 0, 'C')
            pdf.cell(30, 7, cedula, 1, 0, 'C')
            pdf.cell(50, 7, empresa, 1, 0, 'C')
            pdf.cell(40, 7, fecha, 1, 0, 'C')
            pdf.cell(40, 7, f'${valor_pagado:.2f}', 1, 1, 'C')
            total_pagos += 1
            total_valor += valor_pagado
        
        # Resumen del reporte
        pdf.ln(5)  # Espaciado reducido
        pdf.set_x((pdf.w - 250) / 2)  # Centrar la tabla
        pdf.set_font('Times', 'B', 12)
        pdf.cell(250, 10, f'Total de Pagos: {total_pagos}', ln=True, align='L')
        pdf.set_x((pdf.w - 250) / 2)
        pdf.cell(250, 10, f'Valor Total Recibido: ${total_valor:.2f}', ln=True, align='L')
        
        # Guardar el PDF en un bucket S3
        s3 = boto3.client('s3')
        archivo_nombre = f"{FILE_PREFIX}-{ayer_local.replace('/', '-')}.pdf"
        pdf_output = f"/tmp/{archivo_nombre}"
        pdf.output(pdf_output)
        
        with open(pdf_output, 'rb') as data:
            s3.put_object(Bucket=S3_BUCKET, Key=archivo_nombre, Body=data)
        
        return {
            'statusCode': 200,
            'body': f'Reporte generado y guardado en S3: {archivo_nombre}'
        }
    
    finally:
        cursor.close()
        connection.close()
