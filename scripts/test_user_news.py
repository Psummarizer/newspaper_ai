import asyncio
import os
import sys
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(name)s - %(message)s')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.podcast_service import NewsPodcastService

raw_news_data = {
    "POLÍTICA Y GOBIERNO": [
        {
            "titulo": "Peramato asciende al equipo de García Ortiz y deja sin promoción a los fiscales del 'procés'",
            "noticia": "La fiscal general ha buscado equilibrar sensibilidades internas sin contentar a los críticos. En la convocatoria para la sección penal del Supremo, tres de las cuatro jefaturas estaban en juego y también había plazas de menor rango en disputa. Entre los candidatos figuraban García León, jefa de la Secretaría Técnica; Madrigal, ex fiscal general; Moreno y otros tres fiscales que ya ejercen en el Tribunal Supremo. García León, ligada a la UPF y cercana a García Ortiz, encabeza la promoción para las jefaturas de la sección penal del Supremo. A su lado se postulan José Javier Huete, conservador no asociado, y María Farnés, fiscal procedente de Canarias y sin afiliación. En las tres plazas que no son de jefe, Peramato propuso a Villafañe, de UPF y hasta ahora segundo de la Secretaría Técnica, quien lleva seis años participando en decisiones relevantes; Isabel Gómez, de la Fiscalía Provincial de Cuenca y vocal en el Consejo Fiscal por la Asociación de Fiscales (AF); y Antonio Colmenarejo, miembro de la Secretaría Técnica y no asociado. En total concurrían 36 candidatos a estas plazas, entre los que destacaba Lastra, autora de uno de los testimonios más duros contra el ex fiscal general."
        },
        {
            "titulo": "Plus Ultra: la investigación regresa a la Audiencia Nacional",
            "noticia": "La investigación sobre la aerolínea Plus Ultra ha sido devuelta a la Audiencia Nacional por un fallo de competencia en la instrucción de Madrid. El foco de la causa es la posible desviación de los 53 millones de euros que el Gobierno aportó como rescate tras la pandemia. El sumario continúa bajo secreto y las diligencias se reanudan ante el tribunal especializado tras los últimos avances en las pesquisas. La instrucción se activó en 2024 tras la llegada de solicitudes de cooperación internacional procedentes de Francia y Suiza."
        },
        {
            "titulo": "El Gobierno busca reflotar el escudo social y evitar 70.000 desahucios",
            "noticia": "La vicepresidenta primera y ministra de Hacienda, María Jesús Montero, afirmó que el Gobierno analiza alternativas para reactivar el escudo social y mantener la moratoria de desahucios. En una entrevista concedida a la cadena SER, subrayó la necesidad de buscar acuerdos y de escuchar a los distintos actores políticos para avanzar. Pulsar el sentir de los grupos y el diálogo es la mayor ventaja para lograr el bienestar llegue a los ciudadanos, afirmó, al reiterar que se estudiarán opciones."
        }
    ],
    "GEOPOLÍTICA GLOBAL": [
        {
            "titulo": "EE. UU. e Irán avanzan en las conversaciones nucleares, según mediador",
            "noticia": "Un mediador internacional informó que las conversaciones para un acuerdo nuclear entre Washington y Teherán registraron avances significativos en la última ronda de contactos. Paralelamente, Estados Unidos ha intensificado su despliegue militar en la región, con la llegada de dos portaaviones, más de cincuenta aeronaves adicionales y numerosos aviones cisterna, además de destructores, cruceros y submarinos."
        },
        {
            "titulo": "Los precios del petróleo bajan tras extenderse las conversaciones entre Estados Unidos e Irán",
            "noticia": "El mercado petrolero mostró un descenso moderado durante la jornada, con el Brent y el WTI moviéndose en terreno negativo frente a la sesión anterior. Los precios del petróleo retrocedieron de forma modesta ante la prolongación de las negociaciones entre Estados Unidos e Irán. Los mercados valoran la posibilidad de que un acuerdo que estabilice el suministro sirva para mitigar tensiones geopolíticas y, a la vez, moderar la demanda."
        },
        {
            "titulo": "Anthropic acusa al Departamento de Defensa de exigir eliminar salvaguardas para vigilancia masiva y drones autónomos",
            "noticia": "El consejero delegado de Anthropic ha salido al paso de años de rumores al afirmar que hay una tensión con la Administración de Estados Unidos. Según la firma, la actual administración, durante la era de Donald Trump, los ha designado como un 'riesgo' para la seguridad nacional, una etiqueta hasta ahora inédita para una empresa tecnológica estadounidense. Anthropic sostiene que el Gobierno exige eliminar todas las salvaguardas."
        },
        {
            "titulo": "Grecia condena a cuatro implicados por Predatorgate y espionaje con spyware",
            "noticia": "Una corte en Atenas condenó a cuatro individuos, entre ellos dos israelíes, por un escándalo de espionaje que utilizó software de vigilancia para vigilar a políticos, empresarios y periodistas. La sentencia subraya la profundidad del caso conocido como Predatorgate y su impacto en la vida política griega."
        },
        {
            "titulo": "EE.UU. persigue objetivos en Irán que serán difíciles de lograr",
            "noticia": "Analistas señalan que, al analizar mensajes públicos, acciones anteriores y comentarios ocasionales de la Administración, es posible delinear varios objetivos para Irán. Algunos fines parecen realistas y podrían alcanzarse con presión diplomática y medidas limitadas. Otros objetivos serían impracticables o extremadamente costosos en términos de riesgo."
        },
        {
            "titulo": "Reino Unido retira temporalmente a su personal de la embajada en Irán ante posible ataque de EE. UU.",
            "noticia": "La guía del Foreign, Commonwealth and Development Office (FCDO) británico se alinea con las instrucciones estadounidenses para reducir riesgos, incluida la orientación para el personal en Israel. En Irán, el personal británico ha sido retirado temporalmente y la embajada continúa operando de forma remota, mientras Londres mantiene la advertencia de evitar viajes al país ante una posible escalada de tensiones regionales."
        },
        {
            "titulo": "Trump dice no estar satisfecho con Irán tras las últimas negociaciones sobre su programa nuclear",
            "noticia": "El presidente Donald Trump aseguró que no está satisfecho con la postura de Irán en las negociaciones sobre su programa nuclear, que terminaron sin un acuerdo en Ginebra. Reiteró que no quiere recurrir a la fuerza, pero dejó abierta la posibilidad de actuar si persisten las exigencias."
        },
        {
            "titulo": "Trump acelera la amenaza a la Revolución Cubana",
            "noticia": "En Cuba, la retórica revolucionaria convive con una realidad cada vez más dura para la gente común. El descenso de la economía cubana tras la pandemia se ha agravado por un giro internacional que reduce los suministros de petróleo. El endurecimiento de la política exterior estadounidense ha acelerado la reducción de suministros vitales para la isla."
        }
    ],
    "NEGOCIOS Y EMPRESAS": [
        {
            "titulo": "Deducción por compra de coche eléctrico vuelve a caer en el Congreso",
            "noticia": "La deducción del 15% por la adquisición de vehículos electrificados volvió a caer en el Congreso, al no obtener el respaldo necesario dentro del escudo social que se debatía. La medida buscaba incentivar la compra de coches con motor eléctrico."
        },
        {
            "titulo": "Musk advierte: la verdadera ventaja ante la IA está en saber preguntar",
            "noticia": "Un vídeo que ha vuelto a circular en X muestra a Elon Musk señalando un cambio de rumbo: la IA ha transformado el terreno donde se construye conocimiento, desplazando el peso de las respuestas hacia las preguntas que se formulan. Según Musk, el verdadero cuello de botella ya no es la inteligencia ni la labor, sino la capacidad de detectar preguntas que abran nuevas posibilidades."
        },
        {
            "titulo": "Jack Dorsey reduce un 40% de la plantilla de Block y mantiene la contratación de ingenieros de IA",
            "noticia": "El cofundador de Block, Jack Dorsey, comunicó un ajuste drástico: la plantilla pasó de aproximadamente 10.000 personas a poco menos de 6.000. La justificación fue estratégica: la IA está emergiendo como motor de una forma de trabajar más ágil, con equipos más pequeños y menos jerárquicos."
        },
        {
            "titulo": "Sacyr reduce deuda a mínimos históricos y fortalece el flujo de caja pese a desinversiones en Colombia",
            "noticia": "En 2025, Sacyr mostró una ganancia neta atribuible de 86 millones de euros, un 24% menos que el ejercicio anterior, afectada por el impacto contable derivado de la desinversión de tres autopistas en Colombia. Si se excluye ese efecto, el beneficio se elevó un 46% hasta 165 millones, y el flujo de caja operativo avanzó un 5%."
        },
        {
            "titulo": "Anthropic se niega a ceder ante condiciones militares para el uso de Claude",
            "noticia": "Anthropic rechazó las condiciones que exige el Pentágono para el uso de Claude. La firma comunicó su posición ante un plazo para cerrar un acuerdo con el Departamento de Defensa. El director ejecutivo, Dario Amodei, afirmó que la empresa no aceptará cláusulas que atenten contra sus principios."
        }
    ],
    "TRANSPORTE": [
        {
            "titulo": "Tienda de motos en España debe explicar su vinculación con material para los Eurofighter",
            "noticia": "En España operan más de un centenar de aeronaves de combate y de apoyo, entre ellas las destinadas al Eurofighter. Su cadena de suministro es extremadamente especializada y exige que cada pieza cumpla estándares técnicos milimétricos. Una revisión de contratos públicos ha destapado una supuesta venta de material para aeronaves que excede la actividad habitual de la tienda."
        },
        {
            "titulo": "ALA y Aena intensifican el choque por tarifas aeroportuarias y seguridad",
            "noticia": "La discusión de las tarifas aeroportuarias en España ha derivado en un choque público entre ALA y Aena, motivado por las declaraciones de Maurici Lucena. El cruce entre el sector y el gestor de infraestructuras evidencia tensiones sobre la planificación de costes y la seguridad de las operaciones."
        }
    ],
    "TECNOLOGÍA Y DIGITAL": [
        {
            "titulo": "Google presenta Nano Banana 2: IA que genera imágenes realistas en segundos",
            "noticia": "Google ha presentado Nano Banana 2, una actualización de su generador de imágenes impulsado por inteligencia artificial. El nuevo modelo mantiene la filosofía de la versión Pro y busca acelerar la producción sin perder detalle."
        },
        {
            "titulo": "Anthropic defiende salvaguardas en IA para defensa ante el Departamento de Guerra de Estados Unidos",
            "noticia": "Anthropic sostiene que la IA tiene un papel crucial para defender a Estados Unidos y a otras democracias, y que las decisiones militares deben ser tomadas por el Departamento de Guerra de Estados Unidos, no por empresas privadas. Nunca hemos cuestionado operaciones específicas ni tratado de limitar el uso de nuestra tecnología de forma improcedente."
        },
        {
            "titulo": "Grecia condena a cuatro por escándalo de spyware que sacudió al país",
            "noticia": "Una corte ateniense dictó sentencia contra cuatro personas que comercializaban un software de vigilancia tras un amplio escándalo de escuchas acontecido en 2022. El programa, conocido como Predator, permitió espiar a 87 personas, entre ellas ministros, altos mandos militares y periodistas."
        }
    ],
    "CIENCIA E INVESTIGACIÓN": [
        {
            "titulo": "NASA reconfigura Artemis para vuelos más rápidos y una ruta escalonada hacia la Luna",
            "noticia": "La NASA anunció una revisión sustancial de su programa Artemis para avanzar con una cadencia de vuelos más rápida pero con un enfoque de riesgo controlado. El plan revisado propone una progresión escalonada donde cada misión amplía capacidades y valida procesos clave antes de intentar un alunizaje."
        },
        {
            "titulo": "James Webb identifica una galaxia espiral con barra muy temprana en la historia del Universo",
            "noticia": "Un equipo de investigación ha identificado un candidato destacado para una de las primeras galaxias espirales con barra conocidas, denominado COSMOS-74706. Este objeto parece haber existido hace unos 11,5 mil millones de años."
        },
        {
            "titulo": "NASA replantea su ruta lunar e incorpora una misión de prueba en 2027 antes del aterrizaje",
            "noticia": "La NASA ha decidido replantear su estrategia para regresar a la Luna, aceptando que el programa actual no cumple los objetivos previstos. Se plantea aumentar el número de vuelos orbitales antes del aterrizaje humano, previsto inicialmente para 2028."
        },
        {
            "titulo": "Eugenio Manuel Fernández traza el origen y el impacto de la mecánica cuántica",
            "noticia": "El ensayo de Eugenio Manuel Fernández propone una mirada coral a la historia de la mecánica cuántica, desplazando el foco de los nombres habituales hacia voces que han quedado en sombras, como Mileva Marić, Emmy Noether o Chien-Shiung Wu."
        },
        {
            "titulo": "Giro atómico crea estructuras magnéticas gigantes sin electricidad ni productos químicos",
            "noticia": "En materiales magnéticos bidimensionales, el comportamiento colectivo de los espines determina la fase macroscópica. Si todos apuntan en la misma dirección, hablamos de ferromagnetismo; si se alternan, de antiferromagnetismo."
        },
        {
            "titulo": "SEEQC impulsa chips superconductores para una nueva era de la computación cuántica",
            "noticia": "SEEQ, una empresa dedicada a la fabricación de chips cuánticos, sostiene que los circuitos superconductores podrían impulsar una nueva era para la computación cuántica. Su planta de producción de chips está en el norte de Nueva York y recoge el legado del programa de superconductividad de IBM que fue abandonado décadas atrás."
        }
    ],
    "DEPORTES": [
        {
            "titulo": "Honda identifica la causa del fallo que afecta al Aston Martin",
            "noticia": "En Bahréin, durante las pruebas, las vibraciones afectaron el sistema de batería del Aston Martin, según informan desde Honda Racing Corporation. Takeishi, jefe del departamento de carreras, admite que la situación es compleja y que aún no está claro si la batería es el origen del fallo."
        },
        {
            "titulo": "Fernando Alonso impulsa la esperanza: un año de expectativa eterna",
            "noticia": "En el imaginario del automovilismo español, Fernando Alonso sigue siendo más símbolo de esperanza que garantía de victoria. Su historia personal encarna la paciencia ante un deporte cada vez más dependiente de tecnología, simuladores y datos, donde un talento notable puede esforzarse al máximo sin que el resultado acompañe siempre."
        },
        {
            "titulo": "OPTA: la IA sitúa al Arsenal como principal favorito para levantar la Orejona",
            "noticia": "El sistema de predicción de OPTA analiza variables históricas y actuales para estimar las probabilidades en la Champions. OPTA sitúa al Arsenal como principal favorito para levantar la Orejona, mientras otros aspirantes quedan en posiciones inferiores. La IA reduce así las opciones de los equipos españoles, que no lideran la clasificación de favoritos."
        }
    ]
}

async def generate():
    service = NewsPodcastService()
    
    print(f"Total categorias: {len(raw_news_data)}")
    total_news = sum(len(items) for items in raw_news_data.values())
    print(f"Total noticias provistas: {total_news}")
    print("El sistema las evaluará, seleccionará las 10 mejores conservando el orden original y generará el podcast modular.")
    
    output_audio = await service.generate_for_topics(user_id="userNews", topics_news=raw_news_data)
    
    if output_audio:
        print(f"\n✅ Podcast generado con éxito en: {output_audio}")
    else:
        print("\n❌ Error generando el podcast.")

if __name__ == "__main__":
    asyncio.run(generate())
