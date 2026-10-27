import { Client } from "pg";

// SUPER DUPAZA FASE 5.26.6 - POSTGRESQL ONLY APP PATH - POSTGRES PRIMARY READS + DUAL CONTROL WRITES - IMPORTADOR EXCEL API MANUAL / BO+VT PDV / DEDUPE / HOVER
// SUPER DUPAZA D1 - FASE 5.24 REPORT UX + METAS
// Base: Fase 5.23.2 Queue Diagnostic. Mantiene Cron -> Queue -> Consumer.

const APPS_SCRIPT_URL =
  "https://script.google.com/macros/s/AKfycbzl3e1CdxI_zc1sboZv8h7d0GJ-vOXDWoGnI5y72ESQrESNZAiuHnDnB-kp4nxo1Zy3vQ/exec";

const ZAS_BASE = "https://zasconnect.zashome.com.gt";

const ZAS_EXPORT_URL =
  "https://zasconnect.zashome.com.gt/api/orders/export-data?companyCode=DUPAZA";

const SOURCE_CACHE_SECONDS = 300;


// ============================================================
// REGLAS DE NEGOCIO
// ============================================================

const NAME_MAP = new Map(Object.entries({
  "MARIA RENE SANTA CRUZ COSAJAY": "MARIA SANTACRUZ",
  "HENRY ESTUARDO PACHECO ARIANO": "HENRY PACHECO",
  "ROSA LIDIA PEREZ": "ROSA PEREZ",
  "CLEMENCIA ROCIO MIZA": "CLEMENCIA MIZA",
  "GRECIA SARAI FLORES FLORES": "GRECIA FLORES",
  "EVELYN PAOLA DARDON MORATAYA": "EVELYN DARDON",
  "MARVIN FRANCISCO CARRERA PINEDA": "MARVIN CARRERA",
  "YEIMI NOHEMI HERNANDEZ GOMEZ": "YEIMI HERNANDEZ",
  "ANA KARINA VELASQUEZ CALDERON DE CANAS": "ANA VELASQUEZ",
  "JHONATAN ISRAEL BARRIOS MUNOZ": "JHONATAN BARRIOS",
  "CRISTIAN OMAR SANTOS ROSALES": "CRISTIAN SANTOS",
  "LOURDES CAROLINA CAMPOS REYES": "LOURDES CAMPOS",
  "CARLOS ARMANDO GIL TAJIN": "CARLOS GIL",
  "OSCAR DANIEL IXCAYAU AGUILAR": "OSCAR IXCAYAU",
  "SIN ASIGNAR": "SINASIGNAR",
  "BYRON SILVERIO ORTIZ MENDOZA": "BYRON ORTIZ",
  "ESTHER DEL CARMEN LOPEZ CRUZ": "ESTHER LOPEZ",
  "SONIA MARIBEL CHIROY": "SONIA CHIROY",
  "MARVIN DAVID CANEL HERNANDEZ": "MARVIN CANEL",
  "BLANCA RUTILIA YACAB AC": "BLANCA YACAB",
  "PEDRO JOSUE YAX VILLATORO": "PEDRO VILLATORO",
  "VICTOR MIGUEL ANGEL ASTURIAS TZAMOL": "VICTOR ANGEL",
  "ERICKA VICTORIA GONZALEZ LOPEZ": "ERICKA GONZALEZ",
  "KAREN YULISA MENCOS RODAS": "KAREN MENCOS",
  "FABIANA MENDOZA LOPEZ": "FABIANA LOPEZ",
  "STEPHANIE DENNES AJUCHAN HERNANDEZ": "STEPHANIE AJUCHAN",
  "ALISON MELISA AJANEL JOLON": "ALYSON AJANEL",
  "IRMA LOURDES DE PAZ": "LOURDES PAZ",
  "DENIS SEBASTIAN AJUCHAN SANTOS": "DENIS AJUCHAN",
  "SANDRA PATRICIA GUTIERREZ BLANCO": "SANDRA GUTIERREZ",
  "FELIX ANTONIO ACEITUNO BARRIENTOS": "FELIX ACEITUNO",
  "JUAN LUIS COTZAJAY TIJE": "JUAN COTZAJAY",
  "BRIAN SALVADOR CANO RAMIREZ": "BRIAN CANO",
  "JAMNIA GALILEA MORALES ORELLANA": "GALILEA MORALES",
  "WILMER ALEXANDER CAJAS JUAREZ": "WILMER CAJAS",
  "ADA LUZ MARIA MENDEZ SOSA": "ADA LUZ MENDEZ",
  "CHRISTIAN OLIVER GONZALEZ ORDONEZ": "CHRISTIAN GONZALEZ",
  "JESSICA ILEANA GARCIA PALACIOS": "JESSICA GARCIA",
  "GERBER ROMUALDO YAX VILLATORO": "GERBER YAX",
  "HAZEL BARINIA AGUILAR GONZALEZ": "HAZEL AGUILAR",
  "OSCAR JOSUE HERNANDEZ PEREZ": "OSCAR HERNANDEZ",
  "WALTER NEHEMIAS GUERRA": "WALTER GUERRA",
  "YESSICA ALEJANDRA CARRERA PINEDA": "YESSICA CARRERA",
  "HIDELKY AJIN": "HIDELKYAJIN",
  "BYRON ALEXIS MAYOR": "BYRON MAYOR",
  "YARELIN BARRAZA ARIAS": "YARELIN ARIAS",
  "YOSELIN EUFEMIA BARRAZA ARIAS": "YOSELIN BARRAZA",
  "YENDY MIREYA CUMAR CASTRO": "YENDY CUMAR",
  "ANGEL DANIEL LOPEZ PATZAN": "ANGEL LOPEZ",
  "BRYAN ARMANDO LOPEZ PATZAN": "BRYAN LOPEZ",
  "HILLARY GISSELL RUCAL GALLINA": "HILLARY RUCAL",
  "KIMBERLY JESSENIA REGUAN RABAY": "KIMBERLY REGUAN",
  "LUIS DAVID QUEXEL GIL": "LUIS QUEXEL",
  "WENDY ARELIS ALQUIJAY GIL": "WENDY ALQUIJAY",
  "ROSALINDA EUSEBIA RAMIREZ TEZEN": "ROSALINDA RAMIREZ",
  "JULIO ALEJANDRO AJXUP GIL": "JULIO AJXUP",
  "JOCARI ANASOL LOPEZ SICAL": "JOCARI LOPEZ",
  "DIEGO SANTA CRUZ": "DIEGO SANTACRUZ",
  // JUAN MEJIA y GLENDA REINOSO son vendedores distintos.
  // No normalizar uno hacia el otro.
  "ODILIA NINETTE CALEL CARAU": "ODILIA NINETH CALEL",
  "VICENTE PEREZ ARGUETA": "VICENTE PEREZ"
}));

const K1_PETAPA = new Set([
  "JUAN COTZAJAY",
  "FELIX ACEITUNO",
  "VICENTE PEREZ",
  "GLENDA REINOSO"
]);

const K1_XENACOJ = new Set([
  "MARVIN CANEL"
]);

// Desde Fase 5.11, venir por API NO convierte a un vendedor en PDV.
// El segmento manual se aplica en la consulta del reporte y BO/VT PDV
// sigue teniendo prioridad cuando existe.
const API_PDV_PETAPA_SELLERS = new Set([]);

const DIGITAL_K2 = new Set([...NAME_MAP.values()]);

const CHANNELS = [
  "DIGITAL",
  "PETAPA",
  "XENACOJ"
];



// ============================================================
// NORMALIZACIÓN EXCLUSIVA DE ÓRDENES NO CARGADAS
// Fuente: archivos de normalización entregados por el usuario.
// Primera columna de nombres = valor oficial a mostrar.
// ============================================================

const PENDING_NAME_NORMALIZATION = [{"canonical":"ALEJANDRA SIAN","aliases":["ALEJANDRA SIAN"]},{"canonical":"BYRON ORTIZ","aliases":["BYRON ORTIZ","BYRON SILVERIO ORTIZ MENDOZA"]},{"canonical":"ELIDA MARIBEL AQUINO","aliases":["ELIDA MARIBEL AQUINO"]},{"canonical":"ERICK SIQUIBACHE","aliases":["ERICK SIQUIBACHE"]},{"canonical":"ERICKA CHAVEZ","aliases":["ERICKA CHAVEZ","Erika Jannette Chávez Cedillo"]},{"canonical":"FELIX ACEITUNO","aliases":["FELIX ACEITUNO","FELIX  ANTONIO ACEITUNO BARRIENTOS"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso"]},{"canonical":"GRECIA FLORES","aliases":["GRECIA FLORES","GRECIA SARAI FLORES FLORES"]},{"canonical":"HENRY PACHECO","aliases":["HENRY PACHECO","HENRY ESTUARDO PACHECO ARIANO"]},{"canonical":"JHONATAN BARRIOS","aliases":["JHONATAN BARRIOS","JHONATAN ISRAEL BARRIOS MUÑOZ"]},{"canonical":"JOSSELYN ROXANA SANTIAGO","aliases":["JOSSELYN ROXANA SANTIAGO"]},{"canonical":"JUAN COTZAJAY","aliases":["JUAN COTZAJAY","JUAN LUIS COTZAJAY TIJE"]},{"canonical":"MARIA SANTACRUZ","aliases":["MARIA SANTACRUZ","MARIA RENE SANTA CRUZ COSAJAY"]},{"canonical":"MARVIN CANEL","aliases":["MARVIN CANEL","MARVIN DAVID CANEL HERNANDEZ"]},{"canonical":"PAOLA SOLIS","aliases":["PAOLA SOLIS"]},{"canonical":"SULY ASENCIO","aliases":["SULY ASENCIO"]},{"canonical":"VICENTE PEREZ ARGUETA","aliases":["VICENTE PEREZ ARGUETA"]},{"canonical":"VILMA GLORIBEL PEÑA","aliases":["VILMA GLORIBEL PEÑA"]},{"canonical":"WALTER GUERRA","aliases":["WALTER GUERRA","WALTER NEHEMIAS GUERRA"]},{"canonical":"YESSICA CARRERA","aliases":["YESSICA CARRERA","YESSICA ALEJANDRA CARRERA PINEDA"]},{"canonical":"YOSELIN BARRAZA","aliases":["YOSELIN BARRAZA","YOSELIN EUFEMIA BARRAZA ARIAS"]},{"canonical":"ESTHER LOPEZ","aliases":["ESTHER LOPEZ","ESTHER DEL CARMEN LOPEZ CRUZ"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Wendy pacheco"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Jennifer Arroyo"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Loren Nohemí Canahuí Chan"]},{"canonical":"JUAN MEJIA","aliases":["JUAN MEJIA"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Lidia Pérez"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Fabiola Rodas"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Alberto Castro"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Lady Paredes"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Mirna Francisca Chiguichón Sibrian"]},{"canonical":"Glenda Reinoso","aliases":["Glenda Reinoso","Joselyn azucena Lux Rodríguez"]}];

const PENDING_MUNICIPALITIES = [{"municipality":"Guatemala  Barcenas","department":"GUATEMALA"},{"municipality":"Guatemala  Boca del Monte","department":"GUATEMALA"},{"municipality":"Guatemala  Villa Hermosa","department":"GUATEMALA"},{"municipality":"Guatemala Ciudad Peronia","department":"GUATEMALA"},{"municipality":"Guatemala Ruta a El Salvador","department":"GUATEMALA"},{"municipality":"Guatemala Villa Lobos","department":"GUATEMALA"},{"municipality":"Guatemala Zona 01","department":"GUATEMALA"},{"municipality":"Guatemala Zona 02","department":"GUATEMALA"},{"municipality":"Guatemala Zona 03","department":"GUATEMALA"},{"municipality":"Guatemala Zona 04","department":"GUATEMALA"},{"municipality":"Guatemala Zona 05","department":"GUATEMALA"},{"municipality":"Guatemala Zona 06","department":"GUATEMALA"},{"municipality":"Guatemala Zona 07","department":"GUATEMALA"},{"municipality":"Guatemala Zona 08","department":"GUATEMALA"},{"municipality":"Guatemala Zona 10","department":"GUATEMALA"},{"municipality":"Guatemala Zona 11","department":"GUATEMALA"},{"municipality":"Guatemala Zona 12","department":"GUATEMALA"},{"municipality":"Guatemala Zona 13","department":"GUATEMALA"},{"municipality":"Guatemala Zona 14","department":"GUATEMALA"},{"municipality":"Guatemala Zona 17","department":"GUATEMALA"},{"municipality":"Guatemala Zona 19","department":"GUATEMALA"},{"municipality":"Guatemala Zona 21","department":"GUATEMALA"},{"municipality":"Guatemala Zona 24","department":"GUATEMALA"},{"municipality":"Guatemala Zona 15","department":"GUATEMALA"},{"municipality":"Guatemala Zona 16","department":"GUATEMALA"},{"municipality":"Guatemala Zona 18","department":"GUATEMALA"},{"municipality":"Guatemala Zona 25","department":"GUATEMALA"},{"municipality":"Guatemala Zona 09","department":"GUATEMALA"},{"municipality":"Sipacate","department":"ESCUINTLA"},{"municipality":"San José La Máquina","department":"SUCHITEPEQUEZ"},{"municipality":"Honduras","department":"HONDURAS"},{"municipality":"Nicaragua","department":"NICARAGUA"},{"municipality":"Chahal","department":"ALTA VERAPAZ"},{"municipality":"Chisec","department":"ALTA VERAPAZ"},{"municipality":"COBAN","department":"ALTA VERAPAZ"},{"municipality":"Fray Bartolomé de las Casas","department":"ALTA VERAPAZ"},{"municipality":"La Tinta","department":"ALTA VERAPAZ"},{"municipality":"Lanquín","department":"ALTA VERAPAZ"},{"municipality":"Panzós","department":"ALTA VERAPAZ"},{"municipality":"Raxruha","department":"ALTA VERAPAZ"},{"municipality":"San Crístobal Verapaz","department":"ALTA VERAPAZ"},{"municipality":"San Juan Chamelco","department":"ALTA VERAPAZ"},{"municipality":"San Miguel Tucurú","department":"ALTA VERAPAZ"},{"municipality":"San Pedro Carchá","department":"ALTA VERAPAZ"},{"municipality":"Santa Cruz Verapaz","department":"ALTA VERAPAZ"},{"municipality":"Santa María Cahabón","department":"ALTA VERAPAZ"},{"municipality":"Senahú","department":"ALTA VERAPAZ"},{"municipality":"Tactic","department":"ALTA VERAPAZ"},{"municipality":"Tamahú","department":"ALTA VERAPAZ"},{"municipality":"Cubulco","department":"BAJA VERAPAZ"},{"municipality":"Granados","department":"BAJA VERAPAZ"},{"municipality":"Purulhá","department":"BAJA VERAPAZ"},{"municipality":"Rabinal","department":"BAJA VERAPAZ"},{"municipality":"SALAMA","department":"BAJA VERAPAZ"},{"municipality":"San Jerónimo","department":"BAJA VERAPAZ"},{"municipality":"San Miguel Chicaj","department":"BAJA VERAPAZ"},{"municipality":"Santa Cruz El Chol","department":"BAJA VERAPAZ"},{"municipality":"CHIMALTENANGO","department":"CHIMALTENANGO"},{"municipality":"Acatenango","department":"CHIMALTENANGO"},{"municipality":"Comalapa","department":"CHIMALTENANGO"},{"municipality":"El Tejar","department":"CHIMALTENANGO"},{"municipality":"Parramos","department":"CHIMALTENANGO"},{"municipality":"Patzicía","department":"CHIMALTENANGO"},{"municipality":"Patzún","department":"CHIMALTENANGO"},{"municipality":"Pochuta","department":"CHIMALTENANGO"},{"municipality":"San Andrés Itzapa","department":"CHIMALTENANGO"},{"municipality":"San José Poaquil","department":"CHIMALTENANGO"},{"municipality":"San Martín Jilotepeque","department":"CHIMALTENANGO"},{"municipality":"San Pedro Yepocapa","department":"CHIMALTENANGO"},{"municipality":"Santa Apolonia","department":"CHIMALTENANGO"},{"municipality":"Santa Cruz Balanyá","department":"CHIMALTENANGO"},{"municipality":"Tecpán Guatemala","department":"CHIMALTENANGO"},{"municipality":"Zaragoza","department":"CHIMALTENANGO"},{"municipality":"Camotán","department":"CHIQUIMULA"},{"municipality":"CHIQUIMULA","department":"CHIQUIMULA"},{"municipality":"Concepción Las Minas","department":"CHIQUIMULA"},{"municipality":"Esquipulas","department":"CHIQUIMULA"},{"municipality":"Ipala","department":"CHIQUIMULA"},{"municipality":"Jocotán","department":"CHIQUIMULA"},{"municipality":"Olopa","department":"CHIQUIMULA"},{"municipality":"Quetzaltepeque","department":"CHIQUIMULA"},{"municipality":"San Jacinto","department":"CHIQUIMULA"},{"municipality":"San José La Arada","department":"CHIQUIMULA"},{"municipality":"San Juan Ermita","department":"CHIQUIMULA"},{"municipality":"Dolores","department":"PETEN"},{"municipality":"FLORES","department":"PETEN"},{"municipality":"La Libertad (Petén)","department":"PETEN"},{"municipality":"Melchor de Mencos","department":"PETEN"},{"municipality":"Poptún","department":"PETEN"},{"municipality":"San Andrés","department":"PETEN"},{"municipality":"San Benito","department":"PETEN"},{"municipality":"San Francisco","department":"PETEN"},{"municipality":"San José (Petén)","department":"PETEN"},{"municipality":"San Luis","department":"PETEN"},{"municipality":"Santa Ana","department":"PETEN"},{"municipality":"Sayaxché","department":"PETEN"},{"municipality":"QUETZALTENANGO","department":"QUETZALTENANGO"},{"municipality":"Almolonga","department":"QUETZALTENANGO"},{"municipality":"Cabricán","department":"QUETZALTENANGO"},{"municipality":"Cajolá","department":"QUETZALTENANGO"},{"municipality":"Cantel","department":"QUETZALTENANGO"},{"municipality":"Coatepeque","department":"QUETZALTENANGO"},{"municipality":"Colomba","department":"QUETZALTENANGO"},{"municipality":"Concepción Chiquirichapa","department":"QUETZALTENANGO"},{"municipality":"El Palmar","department":"QUETZALTENANGO"},{"municipality":"Flores Costa Cuca","department":"QUETZALTENANGO"},{"municipality":"Génova","department":"QUETZALTENANGO"},{"municipality":"Huitán","department":"QUETZALTENANGO"},{"municipality":"La Esperanza","department":"QUETZALTENANGO"},{"municipality":"Olintepeque","department":"QUETZALTENANGO"},{"municipality":"Palestina de los Altos","department":"QUETZALTENANGO"},{"municipality":"Salcajá","department":"QUETZALTENANGO"},{"municipality":"San Carlos Sija","department":"QUETZALTENANGO"},{"municipality":"San Francisco La Unión","department":"QUETZALTENANGO"},{"municipality":"San Juan Ostuncalco","department":"QUETZALTENANGO"},{"municipality":"San Martín Sacatepequez","department":"QUETZALTENANGO"},{"municipality":"San Mateo","department":"QUETZALTENANGO"},{"municipality":"San Miguel Siquilá","department":"QUETZALTENANGO"},{"municipality":"Sibila","department":"QUETZALTENANGO"},{"municipality":"Zunil","department":"QUETZALTENANGO"},{"municipality":"SANTA CRUZ DEL QUICHE","department":"QUICHE"},{"municipality":"Canillá","department":"QUICHE"},{"municipality":"Chajul","department":"QUICHE"},{"municipality":"Chicamán","department":"QUICHE"},{"municipality":"Chiché","department":"QUICHE"},{"municipality":"Chinique","department":"QUICHE"},{"municipality":"Cunén","department":"QUICHE"},{"municipality":"Ixcán","department":"QUICHE"},{"municipality":"Joyabaj","department":"QUICHE"},{"municipality":"Nebaj","department":"QUICHE"},{"municipality":"Pachalúm","department":"QUICHE"},{"municipality":"Patzité","department":"QUICHE"},{"municipality":"Playa Grande","department":"QUICHE"},{"municipality":"Sacapulas","department":"QUICHE"},{"municipality":"San Andrés Salcabaja","department":"QUICHE"},{"municipality":"San Antonio Ilotenango","department":"QUICHE"},{"municipality":"San Bartolomé Jocotenango","department":"QUICHE"},{"municipality":"San Juan Cotzal","department":"QUICHE"},{"municipality":"San Miguel Uspantán","department":"QUICHE"},{"municipality":"San Pedro Jocopilas","department":"QUICHE"},{"municipality":"Santo Tomás Chichicastenango","department":"QUICHE"},{"municipality":"Zacualpa","department":"QUICHE"},{"municipality":"GUASTATOYA","department":"EL PROGRESO"},{"municipality":"El Jícaro","department":"EL PROGRESO"},{"municipality":"Morazán","department":"EL PROGRESO"},{"municipality":"San Agustín Acasaguastlán","department":"EL PROGRESO"},{"municipality":"San Antonio La Paz","department":"EL PROGRESO"},{"municipality":"Sanarate","department":"EL PROGRESO"},{"municipality":"San Cristobal Acasaguastlán","department":"EL PROGRESO"},{"municipality":"Sansare","department":"EL PROGRESO"},{"municipality":"ESCUINTLA","department":"ESCUINTLA"},{"municipality":"Guanagazapa","department":"ESCUINTLA"},{"municipality":"Iztapa","department":"ESCUINTLA"},{"municipality":"La Democracía","department":"ESCUINTLA"},{"municipality":"La Gomera","department":"ESCUINTLA"},{"municipality":"Masagua","department":"ESCUINTLA"},{"municipality":"Nueva Concepción","department":"ESCUINTLA"},{"municipality":"Palín","department":"ESCUINTLA"},{"municipality":"Pueblo Nuevo Tiquisate","department":"ESCUINTLA"},{"municipality":"San José (Escuintla)","department":"ESCUINTLA"},{"municipality":"Santa Lucía Cotzumalguapa","department":"ESCUINTLA"},{"municipality":"San Vicente Pacaya","department":"ESCUINTLA"},{"municipality":"Siquinalá","department":"ESCUINTLA"},{"municipality":"GUATEMALA","department":"GUATEMALA"},{"municipality":"Amatitlán","department":"GUATEMALA"},{"municipality":"Chinautla","department":"GUATEMALA"},{"municipality":"Chuarrancho","department":"GUATEMALA"},{"municipality":"Fraijanes","department":"GUATEMALA"},{"municipality":"Mixco","department":"GUATEMALA"},{"municipality":"Palencia","department":"GUATEMALA"},{"municipality":"San José del Golfo","department":"GUATEMALA"},{"municipality":"San José Pinula","department":"GUATEMALA"},{"municipality":"San Juan Sacatepequez","department":"GUATEMALA"},{"municipality":"San Miguel Petapa","department":"GUATEMALA"},{"municipality":"San Pedro Ayampuc","department":"GUATEMALA"},{"municipality":"San Pedro Sac. (Guatemala)","department":"GUATEMALA"},{"municipality":"San Raymundo","department":"GUATEMALA"},{"municipality":"Santa Catarina Pinula","department":"GUATEMALA"},{"municipality":"Villa Canales","department":"GUATEMALA"},{"municipality":"Villa Nueva","department":"GUATEMALA"},{"municipality":"El Estor","department":"IZABAL"},{"municipality":"Livingstong","department":"IZABAL"},{"municipality":"Los Amates","department":"IZABAL"},{"municipality":"Morales","department":"IZABAL"},{"municipality":"PUERTO BARRIOS","department":"IZABAL"},{"municipality":"JALAPA","department":"JALAPA"},{"municipality":"Mataquescuintla","department":"JALAPA"},{"municipality":"Monjas","department":"JALAPA"},{"municipality":"San Carlos Alzatate","department":"JALAPA"},{"municipality":"San Luis Jiloptepeque","department":"JALAPA"},{"municipality":"San Manuel Chaparrón","department":"JALAPA"},{"municipality":"San Pedro Pinula","department":"JALAPA"},{"municipality":"ANTIGUA GUATEMALA","department":"SACATEPEQUEZ"},{"municipality":"Ciudad Vieja","department":"SACATEPEQUEZ"},{"municipality":"Jocotenango","department":"SACATEPEQUEZ"},{"municipality":"Magdalena Milpas Altas","department":"SACATEPEQUEZ"},{"municipality":"Pastores","department":"SACATEPEQUEZ"},{"municipality":"San Antonio Aguas Calientes","department":"SACATEPEQUEZ"},{"municipality":"San Bartolomé Milpas Altas","department":"SACATEPEQUEZ"},{"municipality":"San Juan Alotenango","department":"SACATEPEQUEZ"},{"municipality":"San Lucas Sacatepequez","department":"SACATEPEQUEZ"},{"municipality":"San Miguel Dueñas","department":"SACATEPEQUEZ"},{"municipality":"Santa Catarina Barahona","department":"SACATEPEQUEZ"},{"municipality":"Santa Lucía Milpas Altas","department":"SACATEPEQUEZ"},{"municipality":"Santa María de Jesús","department":"SACATEPEQUEZ"},{"municipality":"Santiago Sacatepequez","department":"SACATEPEQUEZ"},{"municipality":"Santo Domingo Xenacoj","department":"SACATEPEQUEZ"},{"municipality":"Sumpango","department":"SACATEPEQUEZ"},{"municipality":"SAN MARCOS","department":"SAN MARCOS"},{"municipality":"Ayutla (Tecún Umán)","department":"SAN MARCOS"},{"municipality":"Catarina","department":"SAN MARCOS"},{"municipality":"Concepción Tutuapa","department":"SAN MARCOS"},{"municipality":"El Quetzal","department":"SAN MARCOS"},{"municipality":"El Rodeo","department":"SAN MARCOS"},{"municipality":"El Tumbador","department":"SAN MARCOS"},{"municipality":"Esquipulas Palo Gordo","department":"SAN MARCOS"},{"municipality":"Ixchiguán","department":"SAN MARCOS"},{"municipality":"La Reforma","department":"SAN MARCOS"},{"municipality":"Malacatán","department":"SAN MARCOS"},{"municipality":"Nuevo Progreso","department":"SAN MARCOS"},{"municipality":"Ocós","department":"SAN MARCOS"},{"municipality":"Pajapita","department":"SAN MARCOS"},{"municipality":"Rio Blanco","department":"SAN MARCOS"},{"municipality":"San Antonio Sacatepequez","department":"SAN MARCOS"},{"municipality":"San Cristobal Cucho","department":"SAN MARCOS"},{"municipality":"San José Ojetenám","department":"SAN MARCOS"},{"municipality":"San Lorenzo (San Marcos)","department":"SAN MARCOS"},{"municipality":"San Miguel Ixtahuacán","department":"SAN MARCOS"},{"municipality":"San Pablo","department":"SAN MARCOS"},{"municipality":"San Pedro Sac. (San Marcos)","department":"SAN MARCOS"},{"municipality":"San Rafael Pie de la Cuesta","department":"SAN MARCOS"},{"municipality":"Sibinal","department":"SAN MARCOS"},{"municipality":"Sipacapa","department":"SAN MARCOS"},{"municipality":"Tacaná","department":"SAN MARCOS"},{"municipality":"Tajumulco","department":"SAN MARCOS"},{"municipality":"Tecún Umán","department":"SAN MARCOS"},{"municipality":"Tejutla","department":"SAN MARCOS"},{"municipality":"CUILAPA","department":"SANTA ROSA"},{"municipality":"Barberena","department":"SANTA ROSA"},{"municipality":"Casillas","department":"SANTA ROSA"},{"municipality":"Chiquimulilla","department":"SANTA ROSA"},{"municipality":"Guazacapán","department":"SANTA ROSA"},{"municipality":"Nueva Santa Rosa","department":"SANTA ROSA"},{"municipality":"Oratorio","department":"SANTA ROSA"},{"municipality":"Pueblo Nuevo Viñas","department":"SANTA ROSA"},{"municipality":"San Juan Tecuaco","department":"SANTA ROSA"},{"municipality":"San Rafael Las Flores","department":"SANTA ROSA"},{"municipality":"Santa Cruz Naranjo","department":"SANTA ROSA"},{"municipality":"Santa María Ixhuatán","department":"SANTA ROSA"},{"municipality":"Santa Rosa de Lima","department":"SANTA ROSA"},{"municipality":"Taxisco","department":"SANTA ROSA"},{"municipality":"HUEHUETENANGO","department":"HUEHUETENANGO"},{"municipality":"Aguacatán","department":"HUEHUETENANGO"},{"municipality":"Chiantla","department":"HUEHUETENANGO"},{"municipality":"Colotenango","department":"HUEHUETENANGO"},{"municipality":"Concepción Huista","department":"HUEHUETENANGO"},{"municipality":"Cuilco","department":"HUEHUETENANGO"},{"municipality":"Jacaltenango","department":"HUEHUETENANGO"},{"municipality":"La Democracia","department":"HUEHUETENANGO"},{"municipality":"La Libertad (Huehue)","department":"HUEHUETENANGO"},{"municipality":"Malacatancito","department":"HUEHUETENANGO"},{"municipality":"Nentón","department":"HUEHUETENANGO"},{"municipality":"San Antonio Huista","department":"HUEHUETENANGO"},{"municipality":"San Gaspar Ixchil","department":"HUEHUETENANGO"},{"municipality":"San Ildefonso Ixtaguacán","department":"HUEHUETENANGO"},{"municipality":"San Juán Atitlán","department":"HUEHUETENANGO"},{"municipality":"San Juan Ixcoy","department":"HUEHUETENANGO"},{"municipality":"San Mateo Ixtatán","department":"HUEHUETENANGO"},{"municipality":"San Miguel Acatán","department":"HUEHUETENANGO"},{"municipality":"San Pedro Necta","department":"HUEHUETENANGO"},{"municipality":"San Pedro Soloma","department":"HUEHUETENANGO"},{"municipality":"San Rafael La Independencia","department":"HUEHUETENANGO"},{"municipality":"San Rafael Petzal","department":"HUEHUETENANGO"},{"municipality":"San Sebastián Coatán","department":"HUEHUETENANGO"},{"municipality":"San Sebastián Huehuetenango","department":"HUEHUETENANGO"},{"municipality":"Santa Ana Huista","department":"HUEHUETENANGO"},{"municipality":"Santa Bárbara (Huehue)","department":"HUEHUETENANGO"},{"municipality":"Santa Cruz Barillas","department":"HUEHUETENANGO"},{"municipality":"Santa Eulalia","department":"HUEHUETENANGO"},{"municipality":"Santiago Chimaltenango","department":"HUEHUETENANGO"},{"municipality":"Tectitán","department":"HUEHUETENANGO"},{"municipality":"Todos Santos Cuchumatán","department":"HUEHUETENANGO"},{"municipality":"Unión Cantinil","department":"HUEHUETENANGO"},{"municipality":"Agua Blanca","department":"JUTIAPA"},{"municipality":"Asunción Mita","department":"JUTIAPA"},{"municipality":"Atescatempa","department":"JUTIAPA"},{"municipality":"Comapa","department":"JUTIAPA"},{"municipality":"Conguaco","department":"JUTIAPA"},{"municipality":"El Adelanto","department":"JUTIAPA"},{"municipality":"El Progreso Jutiapa","department":"JUTIAPA"},{"municipality":"Jalpatagua","department":"JUTIAPA"},{"municipality":"Jerez","department":"JUTIAPA"},{"municipality":"JUTIAPA","department":"JUTIAPA"},{"municipality":"Moyuta","department":"JUTIAPA"},{"municipality":"Pasaco","department":"JUTIAPA"},{"municipality":"Quesada","department":"JUTIAPA"},{"municipality":"San José Acatempa","department":"JUTIAPA"},{"municipality":"Santa Catarina Mita","department":"JUTIAPA"},{"municipality":"Yupitepeque","department":"JUTIAPA"},{"municipality":"Zapotitlán","department":"JUTIAPA"},{"municipality":"RETAULHULEU","department":"RETALHULEU"},{"municipality":"Champerico","department":"RETALHULEU"},{"municipality":"El Asintal","department":"RETALHULEU"},{"municipality":"Nuevo San Carlos","department":"RETALHULEU"},{"municipality":"San Andrés Villaseca","department":"RETALHULEU"},{"municipality":"San Felipe","department":"RETALHULEU"},{"municipality":"San Martín Zapotitlán","department":"RETALHULEU"},{"municipality":"San Sebastián","department":"RETALHULEU"},{"municipality":"Santa Cruz Muluá","department":"RETALHULEU"},{"municipality":"SOLOLA","department":"SOLOLA"},{"municipality":"Concepción","department":"SOLOLA"},{"municipality":"Nahualá","department":"SOLOLA"},{"municipality":"San Andrés Semetabaj","department":"SOLOLA"},{"municipality":"San Antonio Palopó","department":"SOLOLA"},{"municipality":"San José Chacayá","department":"SOLOLA"},{"municipality":"San Juan La Laguna","department":"SOLOLA"},{"municipality":"San Lucas Tolimán","department":"SOLOLA"},{"municipality":"San Marcos La Laguna","department":"SOLOLA"},{"municipality":"San Pablo La Laguna","department":"SOLOLA"},{"municipality":"San Pedro La Laguna","department":"SOLOLA"},{"municipality":"Santa Catarina Ixtahuacán","department":"SOLOLA"},{"municipality":"Santa Catarina Palopó","department":"SOLOLA"},{"municipality":"Santa Clara La Laguna","department":"SOLOLA"},{"municipality":"Santa Cruz La Laguna","department":"SOLOLA"},{"municipality":"Santa Lucía Utatlán","department":"SOLOLA"},{"municipality":"Santa María Visitación","department":"SOLOLA"},{"municipality":"Santiago Atitlán","department":"SOLOLA"},{"municipality":"Panajachel","department":"SOLOLA"},{"municipality":"MAZATENANGO","department":"SUCHITEPEQUEZ"},{"municipality":"Chicacao","department":"SUCHITEPEQUEZ"},{"municipality":"Cuyotenango","department":"SUCHITEPEQUEZ"},{"municipality":"Patulul","department":"SUCHITEPEQUEZ"},{"municipality":"Pueblo Nuevo","department":"SUCHITEPEQUEZ"},{"municipality":"Rio Bravo","department":"SUCHITEPEQUEZ"},{"municipality":"Samayac","department":"SUCHITEPEQUEZ"},{"municipality":"San Antonio Suchitepequez","department":"SUCHITEPEQUEZ"},{"municipality":"San Bernardino","department":"SUCHITEPEQUEZ"},{"municipality":"San Francisco Zapotitlán","department":"SUCHITEPEQUEZ"},{"municipality":"San Gabriel","department":"SUCHITEPEQUEZ"},{"municipality":"San Jose El Idolo","department":"SUCHITEPEQUEZ"},{"municipality":"San Juan Bautista","department":"SUCHITEPEQUEZ"},{"municipality":"San Lorenzo (Suchi)","department":"SUCHITEPEQUEZ"},{"municipality":"San Miguel Panam","department":"SUCHITEPEQUEZ"},{"municipality":"San Pablo Jocopilas","department":"SUCHITEPEQUEZ"},{"municipality":"Santa Bárbara (suchi)","department":"SUCHITEPEQUEZ"},{"municipality":"Santo Domingo Suchitepequez","department":"SUCHITEPEQUEZ"},{"municipality":"Santo Tomás La Unión","department":"SUCHITEPEQUEZ"},{"municipality":"Zunilito","department":"SUCHITEPEQUEZ"},{"municipality":"TOTONICAPAN","department":"TOTONICAPAN"},{"municipality":"Momostenango","department":"TOTONICAPAN"},{"municipality":"San Andrés Xecul","department":"TOTONICAPAN"},{"municipality":"San Bartolo","department":"TOTONICAPAN"},{"municipality":"San Cristóbal Totonicapán","department":"TOTONICAPAN"},{"municipality":"San Francisco El Alto","department":"TOTONICAPAN"},{"municipality":"Santa Lucía La Reforma","department":"TOTONICAPAN"},{"municipality":"Santa María Chiquimula","department":"TOTONICAPAN"},{"municipality":"Cabañas","department":"ZACAPA"},{"municipality":"Estanzuela","department":"ZACAPA"},{"municipality":"Gualán","department":"ZACAPA"},{"municipality":"Huité","department":"ZACAPA"},{"municipality":"La Unión","department":"ZACAPA"},{"municipality":"Rio Hondo","department":"ZACAPA"},{"municipality":"San Diego","department":"ZACAPA"},{"municipality":"Teculután","department":"ZACAPA"},{"municipality":"Usumatlán","department":"ZACAPA"},{"municipality":"ZACAPA","department":"ZACAPA"}];

const PENDING_DEPARTMENTS = ["GUATEMALA","ESCUINTLA","SUCHITEPEQUEZ","HONDURAS","NICARAGUA","ALTA VERAPAZ","BAJA VERAPAZ","CHIMALTENANGO","CHIQUIMULA","PETEN","QUETZALTENANGO","QUICHE","EL PROGRESO","IZABAL","JALAPA","SACATEPEQUEZ","SAN MARCOS","SANTA ROSA","HUEHUETENANGO","JUTIAPA","RETALHULEU","SOLOLA","TOTONICAPAN","ZACAPA"];

const PENDING_MATCH_THRESHOLD = 0.80;


// ============================================================
// WORKER
// ============================================================

export default {

  async fetch(request, env, ctx) {

    const url =
      new URL(request.url);

    try {

      // --------------------------------------------------------
      // DASHBOARD
      // --------------------------------------------------------

      if (url.pathname === "/") {
        return htmlDashboard();
      }


      // --------------------------------------------------------
      // POSTGRES / HYPERDRIVE - SMOKE TEST
      // Solo lectura. No cambia todavía el motor del dashboard.
      // --------------------------------------------------------

      if (
        url.pathname === "/api/postgres-smoke" &&
        request.method === "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );

        if (!auth.ok) {

          return json(
            {
              ok: false,
              error: auth.error
            },
            auth.status
          );

        }

        return json(
          await postgresSmokeTest(
            env
          )
        );

      }



      // --------------------------------------------------------
      // POSTGRES - WRITE SMOKE TEST
      // Hace UPDATE sin cambio dentro de transacción y ROLLBACK.
      // --------------------------------------------------------

      if (
        url.pathname === "/api/postgres-write-smoke" &&
        request.method === "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          await postgresWriteSmokeTest(
            env
          )
        );

      }


      // --------------------------------------------------------
      // POSTGRES SHADOW READS
      // Estas rutas NO sustituyen todavía las rutas D1 normales.
      // Sirven para validar salida equivalente desde Supabase.
      // --------------------------------------------------------

      if (
        url.pathname === "/api/pg/source/status" &&
        request.method === "GET"
      ) {

        return json(
          await pgPublicStatus(
            env
          )
        );

      }


      if (
        url.pathname === "/api/pg/not-loaded" &&
        request.method === "GET"
      ) {

        return json(
          await pgNotLoadedDashboard(
            env
          )
        );

      }


      if (
        url.pathname === "/api/pg/report-config" &&
        request.method === "GET"
      ) {

        return json(
          await pgReportConfigGet(
            env
          )
        );

      }


      if (
        url.pathname === "/api/pg/name-normalization/list" &&
        request.method === "GET"
      ) {

        return json(
          await pgPendingNameRulesList(
            env
          )
        );

      }


      if (
        url.pathname === "/api/pg/seller-targets" &&
        request.method === "GET"
      ) {

        return json(
          await pgSellerTargetsList(
            env,
            url.searchParams
          )
        );

      }


      if (
        url.pathname === "/api/pg/manual-api-import/list" &&
        request.method === "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          await pgManualApiImportList(
            env
          )
        );

      }


      if (
        url.pathname === "/api/pg/manual-sap/list" &&
        request.method === "GET"
      ) {

        return json(
          await pgManualSapList(
            env
          )
        );

      }


      if (
        url.pathname === "/api/pg/summary" &&
        request.method === "GET"
      ) {

        return json(
          await pgSummary(
            env,
            url.searchParams
          )
        );

      }


      if (
        url.pathname === "/api/pg/advisor-summary" &&
        request.method === "GET"
      ) {

        return json(
          await pgAdvisorSummary(
            env,
            url.searchParams
          )
        );

      }


      if (
        url.pathname === "/api/pg/report-options" &&
        request.method === "GET"
      ) {

        return json(
          await pgReportOptions(
            env
          )
        );

      }


      // --------------------------------------------------------
      // STATUS - D1
      // --------------------------------------------------------

      if (url.pathname === "/api/source/status") {

        return json(
          await pgOnlyRead(
            "source/status",
            () => pgPublicStatus(env)
          )
        );

      }


      // --------------------------------------------------------
      // ÓRDENES NO CARGADAS
      // --------------------------------------------------------
      // --------------------------------------------------------
      // ÓRDENES NO CARGADAS - D1
      // --------------------------------------------------------

      if (url.pathname === "/api/not-loaded") {

        return json(
          await pgOnlyRead(
            "not-loaded",
            () => pgNotLoadedDashboard(env)
          )
        );

      }


      // --------------------------------------------------------
      // CONFIGURACIÓN VISUAL DE K1 / K2 / K3
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/report-config" &&
        request.method ===
          "GET"
      ) {

        return json(
          await pgOnlyRead(
            "report-config",
            () => pgReportConfigGet(env)
          )
        );

      }


      if (
        url.pathname ===
          "/api/report-config/labels" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgReportConfigSaveLabels(
            env,
            body
          )
        );

      }


      // --------------------------------------------------------
      // NORMALIZACIÓN DINÁMICA DE NOMBRES - PENDIENTES
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/name-normalization/list" &&
        request.method ===
          "GET"
      ) {

        return json(
          await pgOnlyRead(
            "name-normalization/list",
            () => pgPendingNameRulesList(env)
          )
        );

      }


      if (
        url.pathname ===
          "/api/name-normalization/save" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgPendingNameRuleSavePrimary(
            env,
            body
          )
        );

      }


      if (
        url.pathname ===
          "/api/name-normalization/delete" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgPendingNameRuleDeletePrimary(
            env,
            body?.id
          )
        );

      }


      // --------------------------------------------------------
      // METAS MENSUALES POR VENDEDOR
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/seller-targets" &&
        request.method ===
          "GET"
      ) {

        return json(
          await pgOnlyRead(
            "seller-targets",
            () => pgSellerTargetsList(env, url.searchParams)
          )
        );

      }


      if (
        url.pathname ===
          "/api/seller-targets/save" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgSellerTargetsSavePrimary(
            env,
            body
          )
        );

      }


      // --------------------------------------------------------
      // IMPORTADOR EXCEL -> D1 COMO FUENTE API MANUAL
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/manual-api-import/list" &&
        request.method ===
          "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          await pgManualApiImportList(
            env
          )
        );

      }


      if (
        url.pathname ===
          "/api/manual-api-import" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgManualApiImportRowsPrimary(
            env,
            body
          )
        );

      }


      // --------------------------------------------------------
      // CONTROL MANUAL SAP
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/manual-sap/list" &&
        request.method ===
          "GET"
      ) {

        return json(
          await pgOnlyRead(
            "manual-sap/list",
            () => pgManualSapList(env)
          )
        );

      }


      if (
        url.pathname ===
          "/api/manual-sap/mark" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgManualSapMarkPrimary(
            env,
            body?.trackingCode
          )
        );

      }


      if (
        url.pathname ===
          "/api/manual-sap/resolve" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const body =
          await request.json();


        return json(
          await pgManualSapResolvePrimary(
            env,
            body?.trackingCode
          )
        );

      }


      // --------------------------------------------------------
      // RESUMEN BO / VT
      // --------------------------------------------------------
      // --------------------------------------------------------
      // RESUMEN BO / VT - D1
      // --------------------------------------------------------

      if (url.pathname === "/api/summary") {

        return json(
          await pgOnlyRead(
            "summary",
            () => pgSummary(env, url.searchParams)
          )
        );

      }


      // --------------------------------------------------------
      // RESUMEN POR ASESOR
      // --------------------------------------------------------
      // --------------------------------------------------------
      // RESUMEN POR ASESOR - D1
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/advisor-summary"
      ) {

        return json(
          await pgOnlyRead(
            "advisor-summary",
            () => pgAdvisorSummary(env, url.searchParams)
          )
        );

      }


      // --------------------------------------------------------
      // OPCIONES DE REPORTES - D1
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/report-options"
      ) {

        return json(
          await pgOnlyRead(
            "report-options",
            () => pgReportOptions(env)
          )
        );

      }


      // --------------------------------------------------------
      // AUDITORÍA DE AISLAMIENTO D1
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/postgres-runtime-audit" &&
        request.method ===
          "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error: auth.error
            },
            auth.status
          );

        }


        return json({
          ok: true,
          phase: "5.26.6",
          engine: "POSTGRESQL",
          hyperdrivePresent: Boolean(env.HYPERDRIVE),
          d1BindingStillPresent: Boolean(env.DB),
          d1OperationalReads: false,
          d1OperationalWrites: false,
          d1QueueSync: false,
          d1CronSync: false,
          manualApiImporter: "POSTGRESQL",
          legacyD1Routes: "410_GONE",
          readyForFinalDbBindingRemovalTest: true
        });

      }


      // --------------------------------------------------------
      // D1 RETIRADO DEL CAMINO OPERATIVO - FASE 5.26.6
      // --------------------------------------------------------

      if (
        url.pathname.startsWith(
          "/api/d1/"
        ) ||
        url.pathname ===
          "/d1-admin" ||
        url.pathname ===
          "/d1-export-final"
      ) {

        return json(
          {
            ok: false,
            retired: true,
            engine: "POSTGRESQL",
            error: "D1 fue retirado del camino operativo de SUPER DUPAZA.",
            replacement: "/api/postgres-sync-state"
          },
          410
        );

      }


      // --------------------------------------------------------
      // D1 - RESUMEN BO / VT
      // --------------------------------------------------------
      // D1 - RESUMEN BO / VT
      // Validación previa al cambio del dashboard público.
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/summary"
      ) {

        return json(
          await d1Summary(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - RESUMEN POR ASESOR
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/advisor-summary"
      ) {

        return json(
          await d1AdvisorSummary(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // DIAGNÓSTICO MUNICIPIO / TABLA MunName
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/pending-location-test"
      ) {

        return json({
          ok:
            true,

          input: {
            municipality:
              url.searchParams.get(
                "municipality"
              ) ||
              "",

            department:
              url.searchParams.get(
                "department"
              ) ||
              "",

            zone:
              url.searchParams.get(
                "zone"
              ) ||
              ""
          },

          output:
            normalizePendingLocation(
              url.searchParams.get(
                "municipality"
              ),
              url.searchParams.get(
                "department"
              ),
              url.searchParams.get(
                "zone"
              )
            ),

          mode:
            "EXACT_CATALOG_COMPARE"
        });

      }


      // --------------------------------------------------------
      // TEST MANUAL DE QUEUE -> POSTGRESQL
      // En Fase 5.26.4 el consumidor de super-dupaza-sync
      // ejecuta directamente syncIncrementalToPostgres().
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/postgres-queue-test" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        if (
          !env.SUPER_DUPAZA_SYNC_QUEUE
        ) {

          return json(
            {
              ok:
                false,

              stage:
                "PRODUCER_BINDING",

              error:
                "Falta SUPER_DUPAZA_SYNC_QUEUE."
            },
            500
          );

        }


        const payload = {
          type:
            "SUPER_DUPAZA_POSTGRES_QUEUE_TEST",

          cron:
            "MANUAL_POSTGRES_QUEUE_TEST",

          days:
            7,

          scheduledAt:
            new Date()
              .toISOString(),

          enqueuedAt:
            new Date()
              .toISOString()
        };


        await env
          .SUPER_DUPAZA_SYNC_QUEUE
          .send(
            payload
          );


        return json({
          ok:
            true,

          queued:
            true,

          queue:
            "super-dupaza-sync",

          engine:
            "POSTGRESQL",

          days:
            7,

          note:
            "Mensaje enviado. El consumer ejecutará PostgreSQL/Hyperdrive."
        });

      }


      // --------------------------------------------------------
      // TEST MANUAL DE QUEUE
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/queue-diagnostic"
      ) {

        return json({
          ok:
            true,

          queueBindingPresent:
            Boolean(
              env.SUPER_DUPAZA_SYNC_QUEUE
            ),

          d1BindingPresent:
            Boolean(
              env.DB
            ),

          adminSecretPresent:
            Boolean(
              env.D1_SYNC_TOKEN
            ),

          queueConsumerHandlerPresent:
            true,

          expectedBinding:
            "SUPER_DUPAZA_SYNC_QUEUE",

          queue:
            "super-dupaza-sync",

          note:
            env.SUPER_DUPAZA_SYNC_QUEUE
              ? "Binding productor visible para el Worker."
              : "Falta el binding PRODUCTOR de Queue. El activador Queue solo configura el consumidor."
        });

      }


      if (
        url.pathname ===
        "/api/d1/queue-test" &&
        request.method ===
        "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok:
                false,
              stage:
                "AUTH",
              error:
                auth.error
            },
            auth.status
          );

        }


        if (
          !env.SUPER_DUPAZA_SYNC_QUEUE
        ) {

          return json(
            {
              ok:
                false,
              stage:
                "PRODUCER_BINDING",
              queueBindingPresent:
                false,
              expectedBinding:
                "SUPER_DUPAZA_SYNC_QUEUE",
              queue:
                "super-dupaza-sync",
              error:
                "Falta el binding PRODUCTOR SUPER_DUPAZA_SYNC_QUEUE en el Worker."
            },
            500
          );

        }


        const payload = {
          type:
            "SUPER_DUPAZA_SYNC_TEST",

          cron:
            "MANUAL_QUEUE_TEST",

          days:
            7,

          scheduledAt:
            new Date()
              .toISOString(),

          enqueuedAt:
            new Date()
              .toISOString()
        };


        try {

          await env
            .SUPER_DUPAZA_SYNC_QUEUE
            .send(
              payload
            );

        } catch (error) {

          return json(
            {
              ok:
                false,
              stage:
                "QUEUE_SEND",
              queueBindingPresent:
                true,
              error:
                String(
                  error?.message ||
                  error
                )
            },
            500
          );

        }


        return json({
          ok:
            true,

          stage:
            "ENQUEUED",

          queued:
            true,

          queue:
            "super-dupaza-sync",

          binding:
            "SUPER_DUPAZA_SYNC_QUEUE",

          message:
            "Mensaje enviado. El consumer actual procesa PostgreSQL/Hyperdrive; revisa /api/postgres-sync-state."
        });

      }


      // --------------------------------------------------------
      // D1 - DIAGNÓSTICO POR TRACKING
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/tracking"
      ) {

        return json(
          await d1TrackingDiagnostic(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - REPARAR MATERIALIZACIÓN DE REPORTES
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/d1/report-repair" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok:
                false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const now =
          new Date()
            .toISOString();


        const before =
          await detectReportMaterializationDrift(
            env
          );


        const stats =
          await rebuildReportOrdersFromD1(
            env,
            now
          );


        const after =
          await detectReportMaterializationDrift(
            env
          );


        return json({
          ok:
            true,

          source:
            "D1",

          before,

          after,

          stats
        });

      }


      // --------------------------------------------------------
      // D1 - SALUD DE MATERIALIZACIÓN DE REPORTES
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/report-health"
      ) {

        const repair =
          await ensureReportOrdersFresh(
            env
          );


        return json({
          ok:
            true,

          source:
            "D1",

          repaired:
            repair.repaired,

          driftBefore:
            repair.before,

          drift:
            repair.after,

          crossSourceOverlaps:
            await countCrossSourceOverlaps(
              env
            ),

          crossSourceCollisions:
            await countCrossSourceReportCollisions(
              env
            ),

          trackedApiAttributionMismatches:
            await countTrackedApiAttributionMismatches(
              env
            ),

          pdvDateAttributionMismatches:
            await countPdvDateAttributionMismatches(
              env
            ),

          stats:
            repair.stats ||
            null
        });

      }


      // --------------------------------------------------------
      // D1 - DIAGNÓSTICO CACHE ZASCONNECT POR FECHA
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/zas-day"
      ) {

        return json(
          await d1ZasDay(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - DIAGNÓSTICO BO/VT POR FECHA
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/bo-vt-day"
      ) {

        return json(
          await d1BoVtDay(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - DIAGNÓSTICO ASESOR POR FECHA Y FUENTE
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/advisor-day"
      ) {

        return json(
          await d1AdvisorDay(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - DIAGNÓSTICO PDV BO/VT POR FECHA
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/pdv-day"
      ) {

        return json(
          await d1PdvDay(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - COBERTURA POR FECHA
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/date-counts"
      ) {

        return json(
          await d1DateCounts(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - PENDIENTES, CONTEO Y PÁGINA
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/not-loaded"
      ) {

        return json(
          await d1NotLoaded(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // D1 - DIAGNÓSTICO DEL SCHEDULER / CRON
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/cron-status"
      ) {

        const status =
          await d1PublicStatus(
            env
          );


        return json({
          ok: true,
          scheduler:
            status.scheduler,
          d1LastSyncAt:
            status.lastSyncAt,
          d1AgeMinutes:
            status.ageMinutes
        });

      }


      // --------------------------------------------------------
      // D1 - PRUEBA DE CONEXIÓN
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/d1/test"
      ) {

        return json(
          await d1Test(
            env
          )
        );

      }


      // --------------------------------------------------------
      // D1 - EXPORTADOR DEFINITIVO PARA MIGRACIÓN A SUPABASE
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/d1-export-final"
      ) {

        return d1FinalExportPage();

      }


      if (
        url.pathname ===
          "/api/d1/export-final-page" &&
        request.method ===
          "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok:
                false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          await d1FinalExportPageData(
            env,
            url.searchParams
          )
        );

      }


      // --------------------------------------------------------
      // HYPERDRIVE - PRUEBA DE BINDING
      // No ejecuta SQL todavía. Verifica que el binding exista
      // y que Cloudflare entregue una connectionString utilizable.
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/hyperdrive-test"
      ) {

        return hyperdriveTestPage();

      }


      if (
        url.pathname ===
          "/api/hyperdrive-binding-test" &&
        request.method ===
          "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok:
                false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          hyperdriveBindingInfo(
            env
          )
        );

      }


      // --------------------------------------------------------
      // D1 - ADMIN LOCAL
      // No aparece enlazado desde el dashboard principal.
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/d1-admin"
      ) {

        return d1AdminPage();

      }


      // --------------------------------------------------------
      // POSTGRESQL - SINCRONIZACIÓN MANUAL DE PRUEBA
      // Reutiliza el motor actual de sync mediante una capa D1-compatible,
      // pero escribe exclusivamente en Supabase/PostgreSQL.
      // La Queue normal todavía NO usa esta ruta en Fase 5.26.3.
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/postgres-sync-test" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const days =
          Math.min(
            30,
            Math.max(
              1,
              Number(
                url.searchParams.get(
                  "days"
                ) ||
                7
              ) ||
              7
            )
          );


        return json(
          await syncIncrementalToPostgres(
            env,
            days
          )
        );

      }


      // --------------------------------------------------------
      // POSTGRESQL - ESTADO RESUMIDO DEL SYNC
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/postgres-sync-state" &&
        request.method ===
          "GET"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          await postgresSyncSnapshot(
            env
          )
        );

      }


      // --------------------------------------------------------
      // D1 - SINCRONIZACIÓN INCREMENTAL
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/d1/sync/incremental" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        const days =
          Math.min(
            30,
            Math.max(
              1,
              Number(
                url.searchParams.get(
                  "days"
                ) ||
                7
              ) ||
              7
            )
          );


        return json(
          await syncIncrementalToD1(
            env,
            days
          )
        );

      }


      // --------------------------------------------------------
      // D1 - CARGA INICIAL COMPLETA
      // POST protegido por D1_SYNC_TOKEN.
      // --------------------------------------------------------

      if (
        url.pathname ===
          "/api/d1/sync/full" &&
        request.method ===
          "POST"
      ) {

        const auth =
          checkD1AdminToken(
            request,
            env
          );


        if (!auth.ok) {

          return json(
            {
              ok: false,
              error:
                auth.error
            },
            auth.status
          );

        }


        return json(
          await syncFullToD1(
            env
          )
        );

      }


      // --------------------------------------------------------
      // DIAGNÓSTICO TEMPORAL ZASCONNECT
      // Consulta directa, sin cache.
      // --------------------------------------------------------

      if (
        url.pathname ===
        "/api/zas-debug"
      ) {

        const debug =
          await debugZasExport(
            env
          );


        return json(
          debug
        );

      }


      return new Response(
        "Not Found",
        {
          status: 404
        }
      );


    } catch (error) {

      return json(
        {
          ok: false,

          error:
            String(
              error?.stack ||
              error
            )
        },
        500
      );

    }

  },


  async scheduled(
    controller,
    env,
    ctx
  ) {

    // Workers Free tiene un presupuesto de CPU muy pequeño para Cron.
    // El Cron únicamente publica un mensaje ligero.
    // El trabajo pesado se procesa en el Queue consumer.
    ctx.waitUntil(
      enqueueScheduledSync(
        controller,
        env
      )
    );

  },


  async queue(
    batch,
    env,
    ctx
  ) {

    // Configurar esta Queue con:
    // max_batch_size = 1
    // max_concurrency = 1
    //
    // Así nunca se pisan dos sincronizaciones.
    for (
      const message
      of batch.messages
    ) {

      await runQueuedSync(
        message.body ||
        {},
        env
      );

    }

  }

};




async function enqueueScheduledSync(
  controller,
  env
) {

  if (
    !env.SUPER_DUPAZA_SYNC_QUEUE
  ) {

    throw new Error(
      "Falta el Queue binding SUPER_DUPAZA_SYNC_QUEUE."
    );

  }


  const cron =
    controller?.cron ||
    "";


  const days =
    cron ===
      "17 12 * * *"
      ? 30
      : 7;


  const payload = {
    type:
      "SUPER_DUPAZA_SYNC",

    cron,

    days,

    scheduledAt:
      new Date(
        Number(
          controller?.scheduledTime ||
          Date.now()
        )
      )
        .toISOString(),

    enqueuedAt:
      new Date()
        .toISOString()
  };


  // IMPORTANTE:
  // No leer API, no parsear órdenes y no consultar D1 desde el Cron.
  // Solo encolar. Esto mantiene la invocación programada por debajo
  // del límite de CPU de Workers Free.
  await env
    .SUPER_DUPAZA_SYNC_QUEUE
    .send(
      payload
    );


  console.log(
    "SUPER DUPAZA cron ENQUEUED",
    {
      cron,
      days
    }
  );

}




async function pgSchedulerWrite(
  env,
  {
    mode,
    startedAt,
    successAt = null,
    recordsSeen = 0,
    recordsChanged = 0,
    metadata = {},
    lastError = null
  }
) {

  return withPg(
    env,
    async client => {

      if (
        mode ===
        "START"
      ) {

        await client.query(
`INSERT INTO super_dupaza.sync_state (
  source,
  last_started_at,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json,
  last_error
)
VALUES (
  'SCHEDULER',
  $1,
  NULL,
  0,
  0,
  $2,
  NULL
)
ON CONFLICT(source)
DO UPDATE SET
  last_started_at =
    EXCLUDED.last_started_at,
  metadata_json =
    EXCLUDED.metadata_json,
  last_error =
    NULL`,
          [
            startedAt,
            JSON.stringify(
              metadata
            )
          ]
        );


        return;

      }


      if (
        mode ===
        "SUCCESS"
      ) {

        await client.query(
`INSERT INTO super_dupaza.sync_state (
  source,
  last_started_at,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json,
  last_error
)
VALUES (
  'SCHEDULER',
  $1,
  $2,
  $3,
  $4,
  $5,
  NULL
)
ON CONFLICT(source)
DO UPDATE SET
  last_started_at =
    EXCLUDED.last_started_at,
  last_success_at =
    EXCLUDED.last_success_at,
  records_seen =
    EXCLUDED.records_seen,
  records_changed =
    EXCLUDED.records_changed,
  metadata_json =
    EXCLUDED.metadata_json,
  last_error =
    NULL`,
          [
            startedAt,
            successAt,
            Number(
              recordsSeen ||
              0
            ),
            Number(
              recordsChanged ||
              0
            ),
            JSON.stringify(
              metadata
            )
          ]
        );


        return;

      }


      await client.query(
`INSERT INTO super_dupaza.sync_state (
  source,
  last_started_at,
  records_seen,
  records_changed,
  metadata_json,
  last_error
)
VALUES (
  'SCHEDULER',
  $1,
  0,
  0,
  $2,
  $3
)
ON CONFLICT(source)
DO UPDATE SET
  last_started_at =
    EXCLUDED.last_started_at,
  metadata_json =
    EXCLUDED.metadata_json,
  last_error =
    EXCLUDED.last_error`,
        [
          startedAt,
          JSON.stringify(
            metadata
          ),
          String(
            lastError ||
            ""
          )
            .slice(
              0,
              4000
            )
        ]
      );

    }
  );

}



async function runQueuedSync(
  job,
  env
) {

  const startedAt =
    new Date()
      .toISOString();


  const cron =
    String(
      job?.cron ||
      "QUEUE"
    );


  const requestedDays =
    Number(
      job?.days
    );


  const days =
    requestedDays === 30
      ? 30
      : 7;


  const baseMetadata = {
    cron,
    days,

    scheduledAt:
      job?.scheduledAt ||
      null,

    enqueuedAt:
      job?.enqueuedAt ||
      null,

    engine:
      "POSTGRESQL",

    via:
      "HYPERDRIVE"
  };


  try {

    await pgSchedulerWrite(
      env,
      {
        mode:
          "START",

        startedAt,

        metadata: {
          ...baseMetadata,
          event:
            "QUEUE_START"
        }
      }
    );


    const result =
      await syncIncrementalToPostgres(
        env,
        days
      );


    const finishedAt =
      new Date()
        .toISOString();


    const syncResult =
      result?.sync ||
      {};


    await pgSchedulerWrite(
      env,
      {
        mode:
          "SUCCESS",

        startedAt,

        successAt:
          finishedAt,

        recordsSeen:
          Number(
            syncResult
              ?.zasConnect
              ?.ordersSeen ||
            0
          ),

        recordsChanged:
          Number(
            syncResult
              ?.zasConnect
              ?.changedOrders ||
            0
          ),

        metadata: {
          ...baseMetadata,

          event:
            "QUEUE_SUCCESS",

          transactionCommitted:
            Boolean(
              result
                ?.transactionCommitted
            ),

          d1Modified:
            Boolean(
              result
                ?.d1Modified
            ),

          dbQueriesEstimated:
            Number(
              syncResult
                ?.d1QueriesEstimated ||
              0
            )
        }
      }
    );


    console.log(
      "SUPER DUPAZA queue PostgreSQL sync OK",
      {
        cron,
        days,
        startedAt,
        finishedAt,
        transactionCommitted:
          Boolean(
            result
              ?.transactionCommitted
          )
      }
    );


  } catch (error) {

    const failedAt =
      new Date()
        .toISOString();


    const message =
      String(
        error?.stack ||
        error
      )
        .slice(
          0,
          4000
        );


    try {

      await pgSchedulerWrite(
        env,
        {
          mode:
            "ERROR",

          startedAt,

          metadata: {
            ...baseMetadata,
            event:
              "QUEUE_ERROR",
            failedAt
          },

          lastError:
            message
        }
      );

    } catch {
      // No ocultar el error original.
    }


    console.error(
      "SUPER DUPAZA queue PostgreSQL sync ERROR",
      {
        cron,
        days,
        error:
          message
      }
    );


    // Propagar para que Cloudflare Queue aplique retry.
    throw error;

  }

}



// ============================================================
// D1 - FASE 2
// ============================================================

function checkD1AdminToken(
  request,
  env
) {

  if (
    !env.D1_SYNC_TOKEN
  ) {

    return {
      ok: false,
      status: 500,
      error:
        "Falta configurar el Secret D1_SYNC_TOKEN en Cloudflare."
    };

  }


  const supplied =
    request.headers.get(
      "X-Super-Dupaza-Admin"
    ) ||
    "";


  if (
    supplied !==
    env.D1_SYNC_TOKEN
  ) {

    return {
      ok: false,
      status: 401,
      error:
        "Token administrativo incorrecto."
    };

  }


  return {
    ok: true
  };

}




function d1RequireDateRange(
  params
) {

  const from =
    params.get(
      "date_from"
    );


  const to =
    params.get(
      "date_to"
    );


  if (
    !from ||
    !to
  ) {

    throw new Error(
      "Debes enviar date_from y date_to en formato YYYY-MM-DD."
    );

  }


  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(
      from
    ) ||
    !/^\d{4}-\d{2}-\d{2}$/.test(
      to
    )
  ) {

    throw new Error(
      "Formato de fecha inválido. Usa YYYY-MM-DD."
    );

  }


  if (
    from >
    to
  ) {

    throw new Error(
      "date_from no puede ser mayor que date_to."
    );

  }


  return {
    from,
    to
  };

}



function validTargetMonth(
  value
) {

  const month =
    String(
      value ||
      ""
    )
      .trim();


  if (
    !/^\d{4}-\d{2}$/.test(
      month
    )
  ) {

    throw new Error(
      "Mes inválido. Usa YYYY-MM."
    );

  }


  const parts =
    month
      .split(
        "-"
      )
      .map(Number);


  if (
    parts[1] < 1 ||
    parts[1] > 12
  ) {

    throw new Error(
      "Mes inválido."
    );

  }


  return month;

}



function monthKeyFromDate(
  value
) {

  return String(
    value ||
    ""
  )
    .slice(
      0,
      7
    );

}



function daysInMonthKey(
  monthKey
) {

  const [
    year,
    month
  ] =
    monthKey
      .split("-")
      .map(Number);


  return new Date(
    Date.UTC(
      year,
      month,
      0
    )
  )
    .getUTCDate();

}



function rangeMonthKeys(
  from,
  to
) {

  const [
    fromYear,
    fromMonth
  ] =
    from
      .slice(0, 7)
      .split("-")
      .map(Number);


  const [
    toYear,
    toMonth
  ] =
    to
      .slice(0, 7)
      .split("-")
      .map(Number);


  const result = [];

  let year =
    fromYear;

  let month =
    fromMonth;


  while (
    year < toYear ||
    (
      year === toYear &&
      month <= toMonth
    )
  ) {

    result.push(
      String(year) +
      "-" +
      String(month)
        .padStart(2, "0")
    );


    month += 1;


    if (
      month > 12
    ) {

      month = 1;
      year += 1;

    }

  }


  return result;

}



function selectedDaysInMonth(
  from,
  to,
  monthKey
) {

  const start =
    monthKey +
    "-01";


  const end =
    monthKey +
    "-" +
    String(
      daysInMonthKey(
        monthKey
      )
    )
      .padStart(2, "0");


  const overlapStart =
    from > start
      ? from
      : start;


  const overlapEnd =
    to < end
      ? to
      : end;


  if (
    overlapStart >
    overlapEnd
  ) {

    return 0;

  }


  const startMs =
    Date.parse(
      overlapStart +
      "T00:00:00Z"
    );


  const endMs =
    Date.parse(
      overlapEnd +
      "T00:00:00Z"
    );


  return Math.floor(
    (
      endMs -
      startMs
    ) /
    86400000
  ) + 1;

}



function guatemalaMonthKey() {

  const parts =
    new Intl.DateTimeFormat(
      "en-CA",
      {
        timeZone:
          "America/Guatemala",
        year:
          "numeric",
        month:
          "2-digit"
      }
    )
      .formatToParts(
        new Date()
      );


  const year =
    parts.find(
      part =>
        part.type ===
        "year"
    )?.value;


  const month =
    parts.find(
      part =>
        part.type ===
        "month"
    )?.value;


  return (
    year +
    "-" +
    month
  );

}



async function sellerTargetsList(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const month =
    validTargetMonth(
      params.get(
        "month"
      ) ||
      guatemalaMonthKey()
    );


  const [
    sellersResult,
    targetResult
  ] =
    await env.DB.batch([

      env.DB.prepare(
`SELECT seller_name, priority
FROM (
  SELECT advisor AS seller_name, 1 AS priority
  FROM report_orders
  WHERE advisor IS NOT NULL
    AND TRIM(advisor) <> ''

  UNION ALL

  SELECT seller_name, 2 AS priority
  FROM zas_orders
  WHERE seller_name IS NOT NULL
    AND TRIM(seller_name) <> ''

  UNION ALL

  SELECT canonical_name AS seller_name, 3 AS priority
  FROM pending_name_normalization
  WHERE canonical_name IS NOT NULL
    AND TRIM(canonical_name) <> ''
)
ORDER BY priority, seller_name COLLATE NOCASE`
      ),

      env.DB
        .prepare(
`SELECT
  seller_key,
  seller_name,
  target_amount
FROM seller_monthly_targets
WHERE month = ?1
ORDER BY seller_name COLLATE NOCASE`
        )
        .bind(
          month
        )

    ]);


  const targetMap =
    new Map(
      (
        targetResult?.results ||
        []
      )
        .map(
          row => [
            row.seller_key,
            row
          ]
        )
    );


  const sellers =
    new Map();


  for (
    const row
    of (
      sellersResult?.results ||
      []
    )
  ) {

    const name =
      String(
        row.seller_name ||
        ""
      )
        .trim();


    const key =
      normalizeMatchText(
        name
      );


    if (
      !name ||
      !key ||
      sellers.has(
        key
      )
    ) {

      continue;

    }


    sellers.set(
      key,
      name
    );

  }


  for (
    const row
    of (
      targetResult?.results ||
      []
    )
  ) {

    if (
      !sellers.has(
        row.seller_key
      )
    ) {

      sellers.set(
        row.seller_key,
        row.seller_name
      );

    }

  }


  const rows =
    [
      ...sellers.entries()
    ]
      .map(
        ([
          sellerKey,
          seller
        ]) => ({
          sellerKey,
          seller,
          target:
            round2(
              numberValue(
                targetMap.get(
                  sellerKey
                )?.target_amount
              )
            )
        })
      )
      .sort(
        (a, b) =>
          a.seller.localeCompare(
            b.seller,
            "es"
          )
      );


  return {
    ok: true,
    source:
      "D1",
    month,
    rows
  };

}



async function sellerTargetsSave(
  env,
  body
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const month =
    validTargetMonth(
      body?.month
    );


  const incoming =
    Array.isArray(
      body?.targets
    )
      ? body.targets
      : [];


  const now =
    new Date()
      .toISOString();


  const statements = [];


  for (
    const item
    of incoming
  ) {

    const seller =
      String(
        item?.seller ||
        ""
      )
        .trim();


    const sellerKey =
      normalizeMatchText(
        seller
      );


    const amount =
      Number(
        item?.amount ||
        0
      );


    if (
      !seller ||
      !sellerKey ||
      !Number.isFinite(
        amount
      ) ||
      amount < 0
    ) {

      continue;

    }


    if (
      amount <= 0
    ) {

      statements.push(
        env.DB
          .prepare(
`DELETE FROM seller_monthly_targets
WHERE seller_key = ?1
  AND month = ?2`
          )
          .bind(
            sellerKey,
            month
          )
      );

      continue;

    }


    statements.push(
      env.DB
        .prepare(
`INSERT INTO seller_monthly_targets (
  seller_key,
  seller_name,
  month,
  target_amount,
  updated_at
)
VALUES (
  ?1,?2,?3,?4,?5
)
ON CONFLICT(seller_key, month)
DO UPDATE SET
  seller_name = excluded.seller_name,
  target_amount = excluded.target_amount,
  updated_at = excluded.updated_at`
        )
        .bind(
          sellerKey,
          seller,
          month,
          round2(
            amount
          ),
          now
        )
    );

  }


  if (
    statements.length
  ) {

    await env.DB.batch(
      statements
    );

  }


  return {
    ok: true,
    month,
    saved:
      statements.length,
    message:
      "Metas mensuales guardadas."
  };

}



async function sellerTargetsForRange(
  env,
  from,
  to
) {

  const months =
    rangeMonthKeys(
      from,
      to
    );


  if (
    !months.length
  ) {

    return {
      targets:
        new Map(),
      sellers:
        new Map()
    };

  }


  const placeholders =
    months
      .map(
        (_, index) =>
          "?" +
          (
            index + 1
          )
      )
      .join(",");


  const result =
    await env.DB
      .prepare(
`SELECT
  seller_key,
  seller_name,
  month,
  target_amount
FROM seller_monthly_targets
WHERE month IN (${placeholders})`
      )
      .bind(
        ...months
      )
      .all();


  const map =
    new Map();

  const sellers =
    new Map();


  for (
    const row
    of (
      result?.results ||
      []
    )
  ) {

    if (
      !map.has(
        row.seller_key
      )
    ) {

      map.set(
        row.seller_key,
        new Map()
      );

    }


    map.get(
      row.seller_key
    )
      .set(
        row.month,
        numberValue(
          row.target_amount
        )
      );


    if (
      !sellers.has(
        row.seller_key
      )
    ) {

      sellers.set(
        row.seller_key,
        row.seller_name ||
        row.seller_key
      );

    }

  }


  return {
    targets:
      map,
    sellers
  };

}



function proratedTargetForRange(
  sellerName,
  from,
  to,
  targetMap
) {

  const sellerKey =
    normalizeMatchText(
      sellerName
    );


  const sellerTargets =
    targetMap.get(
      sellerKey
    ) ||
    new Map();


  let total = 0;


  for (
    const monthKey
    of rangeMonthKeys(
      from,
      to
    )
  ) {

    const monthlyTarget =
      numberValue(
        sellerTargets.get(
          monthKey
        )
      );


    if (
      !monthlyTarget
    ) {

      continue;

    }


    const selectedDays =
      selectedDaysInMonth(
        from,
        to,
        monthKey
      );


    const daysInMonth =
      daysInMonthKey(
        monthKey
      );


    total +=
      monthlyTarget /
      daysInMonth *
      selectedDays;

  }


  return round2(
    total
  );

}



async function summaryTargetsByLevel(
  env,
  params,
  from,
  to
) {

  const targetInfo =
    await sellerTargetsForRange(
      env,
      from,
      to
    );


  const matchResult =
    await env.DB
      .prepare(
`SELECT
  match_key,
  k_level,
  segment
FROM seller_report_match`
      )
      .all();


  const classification =
    new Map(
      (
        matchResult?.results ||
        []
      )
        .map(
          row => [
            row.match_key,
            {
              level:
                row.k_level ||
                "K3",
              segment:
                row.segment ||
                "DIGITAL"
            }
          ]
        )
    );


  const selectedGroup =
    String(
      params.get(
        "group"
      ) ||
      ""
    )
      .trim();


  const selectedAdvisor =
    normalizeMatchText(
      params.get(
        "advisor"
      ) ||
      ""
    );


  const selectedSegment =
    String(
      params.get(
        "type"
      ) ||
      ""
    )
      .trim();


  const totals = {
    K1: 0,
    K2: 0,
    K3: 0
  };


  for (
    const [
      sellerKey,
      sellerName
    ]
    of targetInfo.sellers.entries()
  ) {

    if (
      selectedAdvisor &&
      selectedAdvisor !==
        sellerKey
    ) {
      continue;
    }


    const rule =
      classification.get(
        sellerKey
      ) ||
      {
        level:
          "K3",
        segment:
          "DIGITAL"
      };


    const level =
      [
        "K1",
        "K2",
        "K3"
      ]
        .includes(
          rule.level
        )
          ? rule.level
          : "K3";


    const segment =
      [
        "DIGITAL",
        "PETAPA",
        "XENACOJ"
      ]
        .includes(
          rule.segment
        )
          ? rule.segment
          : "DIGITAL";


    if (
      selectedGroup &&
      selectedGroup !==
        level
    ) {
      continue;
    }


    if (
      selectedSegment &&
      selectedSegment !==
        segment
    ) {
      continue;
    }


    totals[
      level
    ] +=
      proratedTargetForRange(
        sellerName,
        from,
        to,
        targetInfo.targets
      );

  }


  return {
    K1:
      round2(
        totals.K1
      ),
    K2:
      round2(
        totals.K2
      ),
    K3:
      round2(
        totals.K3
      )
  };

}



function trendForCurrentMonth(
  amount,
  from,
  to
) {

  const fromMonth =
    monthKeyFromDate(
      from
    );


  const toMonth =
    monthKeyFromDate(
      to
    );


  const currentMonth =
    guatemalaMonthKey();


  if (
    fromMonth !== toMonth ||
    fromMonth !== currentMonth
  ) {

    return 0;

  }


  const selectedDays =
    selectedDaysInMonth(
      from,
      to,
      currentMonth
    );


  if (
    selectedDays <= 0
  ) {

    return 0;

  }


  return round2(
    numberValue(
      amount
    ) /
    selectedDays *
    daysInMonthKey(
      currentMonth
    )
  );

}



async function d1Summary(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  await ensureReportOrdersFresh(
    env
  );


  const labelInfo =
    await getReportGroupLabels(
      env
    );


  const filter =
    d1BuildReportFilter(
      params,
      "effective_level",
      "effective_segment"
    );


  const effectiveCte =
    sellerReportEffectiveCteSql();


  const matrixSql =
effectiveCte +
`SELECT
  effective_level AS level,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'DIGITAL'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS DIGITAL,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'PETAPA'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS PETAPA,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'XENACOJ'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS XENACOJ,

  ROUND(
    SUM(amount),
    2
  ) AS TOTAL

FROM effective

WHERE ${filter.where}

GROUP BY effective_level

ORDER BY
  CASE effective_level
    WHEN 'K1' THEN 1
    WHEN 'K2' THEN 2
    WHEN 'K3' THEN 3
    ELSE 99
  END`;


  const chartSql =
effectiveCte +
`SELECT
  report_date AS date,

  ROUND(
    SUM(
      CASE
        WHEN effective_level = 'K1'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS amountK1,

  ROUND(
    SUM(
      CASE
        WHEN effective_level = 'K2'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS amountK2,

  ROUND(
    SUM(
      CASE
        WHEN effective_level = 'K3'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS amountK3,

  ROUND(
    SUM(amount),
    2
  ) AS total

FROM effective

WHERE ${filter.where}

GROUP BY report_date

ORDER BY report_date`;


  const kpiSql =
effectiveCte +
`SELECT
  COUNT(*) AS orders,

  ROUND(
    SUM(
      CASE
        WHEN source = 'ZASCONNECT'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS zasConnect,

  ROUND(
    SUM(
      CASE
        WHEN source = 'PDV BO/VT'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS pdvAdded,

  ROUND(
    SUM(amount),
    2
  ) AS total

FROM effective

WHERE ${filter.where}`;


  const [
    matrixResult,
    chartResult,
    kpiResultRaw
  ] =
    await env.DB.batch([

      d1BindStatement(
        env.DB.prepare(
          matrixSql
        ),
        filter.values
      ),

      d1BindStatement(
        env.DB.prepare(
          chartSql
        ),
        filter.values
      ),

      d1BindStatement(
        env.DB.prepare(
          kpiSql
        ),
        filter.values
      )

    ]);


  const byLevel =
    new Map(
      (
        matrixResult?.results ||
        []
      )
        .map(
          row => [
            row.level,
            row
          ]
        )
    );


  const targetsByLevel =
    await summaryTargetsByLevel(
      env,
      params,
      filter.from,
      filter.to
    );


  const rows =
    [
      "K1",
      "K2",
      "K3"
    ]
      .map(
        level => {

          const source =
            byLevel.get(
              level
            ) ||
            {};


          return {
            level,

            grupo:
              labelInfo.labels[
                level
              ] ||
              level,

            DIGITAL:
              round2(
                numberValue(
                  source.DIGITAL
                )
              ),

            PETAPA:
              round2(
                numberValue(
                  source.PETAPA
                )
              ),

            XENACOJ:
              round2(
                numberValue(
                  source.XENACOJ
                )
              ),

            TOTAL:
              round2(
                numberValue(
                  source.TOTAL
                )
              ),

            META:
              round2(
                targetsByLevel[
                  level
                ] ||
                0
              ),

            TENDENCIA:
              trendForCurrentMonth(
                numberValue(
                  source.TOTAL
                ),
                filter.from,
                filter.to
              )
          };

        }
      );


  rows.push({
    level:
      "TOTAL",

    grupo:
      "RESUMEN",

    DIGITAL:
      round2(
        sum(
          rows.map(
            row =>
              row.DIGITAL
          )
        )
      ),

    PETAPA:
      round2(
        sum(
          rows.map(
            row =>
              row.PETAPA
          )
        )
      ),

    XENACOJ:
      round2(
        sum(
          rows.map(
            row =>
              row.XENACOJ
          )
        )
      ),

    TOTAL:
      round2(
        sum(
          rows.map(
            row =>
              row.TOTAL
          )
        )
      ),

    META:
      round2(
        sum(
          rows.map(
            row =>
              row.META
          )
        )
      ),

    TENDENCIA:
      trendForCurrentMonth(
        sum(
          rows.map(
            row =>
              row.TOTAL
          )
        ),
        filter.from,
        filter.to
      )
  });


  const chart =
    (
      chartResult?.results ||
      []
    )
      .map(
        row => {

          const total =
            numberValue(
              row.total
            );


          const k1 =
            numberValue(
              row.amountK1
            );


          const k2 =
            numberValue(
              row.amountK2
            );


          const k3 =
            numberValue(
              row.amountK3
            );


          return {
            date:
              row.date,

            K1:
              total
                ? round2(
                    k1 /
                    total *
                    100
                  )
                : 0,

            K2:
              total
                ? round2(
                    k2 /
                    total *
                    100
                  )
                : 0,

            K3:
              total
                ? round2(
                    k3 /
                    total *
                    100
                  )
                : 0,

            amountK1:
              round2(
                k1
              ),

            amountK2:
              round2(
                k2
              ),

            amountK3:
              round2(
                k3
              ),

            total:
              round2(
                total
              )
          };

        }
      );


  const kpi =
    (
      kpiResultRaw?.results ||
      []
    )[0] ||
    {};


  return {
    ok: true,

    source:
      "D1",

    range: {
      from:
        filter.from,

      to:
        filter.to
    },

    groupLabels:
      labelInfo.labels,

    rows,

    chart,

    kpis: {
      zasConnect:
        round2(
          numberValue(
            kpi.zasConnect
          )
        ),

      pdvAdded:
        round2(
          numberValue(
            kpi.pdvAdded
          )
        ),

      total:
        round2(
          numberValue(
            kpi.total
          )
        ),

      orders:
        Number(
          kpi.orders ||
          0
        ),

      target:
        round2(
          rows[
            rows.length - 1
          ]?.META ||
          0
        ),

      trend:
        round2(
          rows[
            rows.length - 1
          ]?.TENDENCIA ||
          0
        )
    }
  };

}



async function d1AdvisorSummary(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  await ensureReportOrdersFresh(
    env
  );


  const {
    from,
    to
  } =
    d1RequireDateRange(
      params
    );


  const sql =
sellerReportEffectiveCteSql() +
`SELECT
  advisor AS ASESOR,

  ROUND(
    SUM(amount),
    2
  ) AS MONTO,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'DIGITAL'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS DIGITAL,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'PETAPA'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS PETAPA,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'XENACOJ'
        THEN amount
        ELSE 0
      END
    ),
    2
  ) AS XENACOJ

FROM effective

WHERE report_date
BETWEEN ?1 AND ?2

GROUP BY advisor

ORDER BY MONTO DESC,
         ASESOR ASC`;


  const result =
    await env.DB
      .prepare(
        sql
      )
      .bind(
        from,
        to
      )
      .all();


  const targetInfo =
    await sellerTargetsForRange(
      env,
      from,
      to
    );


  const targetMap =
    targetInfo.targets;


  const rows =
    (
      result?.results ||
      []
    )
      .map(
        row => ({
          ASESOR:
            row.ASESOR,

          MONTO:
            round2(
              numberValue(
                row.MONTO
              )
            ),

          DIGITAL:
            round2(
              numberValue(
                row.DIGITAL
              )
            ),

          PETAPA:
            round2(
              numberValue(
                row.PETAPA
              )
            ),

          XENACOJ:
            round2(
              numberValue(
                row.XENACOJ
              )
            ),

          META:
            proratedTargetForRange(
              row.ASESOR,
              from,
              to,
              targetMap
            ),

          TENDENCIA:
            trendForCurrentMonth(
              row.MONTO,
              from,
              to
            )
        })
      );


  const existingSellerKeys =
    new Set(
      rows.map(
        row =>
          normalizeMatchText(
            row.ASESOR
          )
      )
    );


  for (
    const [
      sellerKey,
      sellerName
    ]
    of targetInfo.sellers.entries()
  ) {

    if (
      existingSellerKeys.has(
        sellerKey
      )
    ) {
      continue;
    }


    rows.push({
      ASESOR:
        sellerName,
      MONTO:
        0,
      DIGITAL:
        0,
      PETAPA:
        0,
      XENACOJ:
        0,
      META:
        proratedTargetForRange(
          sellerName,
          from,
          to,
          targetMap
        ),
      TENDENCIA:
        0
    });

  }


  rows.sort(
    (a, b) =>
      Number(
        b.MONTO ||
        0
      ) -
      Number(
        a.MONTO ||
        0
      ) ||
      String(
        a.ASESOR ||
        ""
      )
        .localeCompare(
          String(
            b.ASESOR ||
            ""
          ),
          "es"
        )
  );


  const totalRow = {
    ASESOR:
      "TOTAL",

    MONTO:
      round2(
        sum(
          rows.map(
            row =>
              row.MONTO
          )
        )
      ),

    DIGITAL:
      round2(
        sum(
          rows.map(
            row =>
              row.DIGITAL
          )
        )
      ),

    PETAPA:
      round2(
        sum(
          rows.map(
            row =>
              row.PETAPA
          )
        )
      ),

    XENACOJ:
      round2(
        sum(
          rows.map(
            row =>
              row.XENACOJ
          )
        )
      ),

    META:
      round2(
        sum(
          rows.map(
            row =>
              row.META
          )
        )
      ),

    TENDENCIA:
      trendForCurrentMonth(
        sum(
          rows.map(
            row =>
              row.MONTO
          )
        ),
        from,
        to
      )
  };


  rows.push(
    totalRow
  );


  return {
    ok: true,

    source:
      "D1",

    range: {
      from,
      to
    },

    rows,

    kpis: {
      advisors:
        Math.max(
          0,
          rows.length -
          1
        ),

      total:
        totalRow.MONTO,

      digital:
        totalRow.DIGITAL,

      petapa:
        totalRow.PETAPA,

      xenacoj:
        totalRow.XENACOJ,

      target:
        totalRow.META,

      trend:
        totalRow.TENDENCIA
    }
  };

}








async function d1TrackingDiagnostic(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const tracking =
    normId(
      params.get(
        "tracking"
      )
    );


  if (!tracking) {

    throw new Error(
      "Envía tracking=CODIGO."
    );

  }


  const [
    zas,
    loaded,
    report
  ] =
    await env.DB.batch([

      env.DB
        .prepare(
`SELECT *
FROM zas_orders
WHERE tracking_code = ?1`
        )
        .bind(
          tracking
        ),

      env.DB
        .prepare(
`SELECT *
FROM loaded_refs
WHERE tracking_code = ?1`
        )
        .bind(
          tracking
        ),

      env.DB
        .prepare(
`SELECT *
FROM report_orders
WHERE tracking_code = ?1`
        )
        .bind(
          tracking
        )

    ]);


  return {
    ok:
      true,

    source:
      "D1",

    tracking,

    zas_orders:
      zas?.results ||
      [],

    loaded_refs:
      loaded?.results ||
      [],

    report_orders:
      report?.results ||
      []
  };

}



async function d1ZasDay(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const date =
    String(
      params.get(
        "date"
      ) ||
      ""
    )
      .trim();


  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(
      date
    )
  ) {

    throw new Error(
      "Envía date=YYYY-MM-DD."
    );

  }


  const result =
    await env.DB
      .prepare(
`SELECT
  tracking_code,
  seller_name,
  order_date,
  order_total,
  source_updated_at,
  synced_at
FROM zas_orders
WHERE order_date = ?1
ORDER BY
  seller_name,
  tracking_code`
      )
      .bind(
        date
      )
      .all();


  const rows =
    result?.results ||
    [];


  return {
    ok: true,

    source:
      "D1",

    date,

    rows,

    count:
      rows.length,

    total:
      round2(
        sum(
          rows.map(
            row =>
              row.order_total
          )
        )
      )
  };

}



async function d1BoVtDay(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const date =
    String(
      params.get(
        "date"
      ) ||
      ""
    )
      .trim();


  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(
      date
    )
  ) {

    throw new Error(
      "Envía date=YYYY-MM-DD."
    );

  }


  const result =
    await env.DB
      .prepare(
`SELECT
  tracking_code,
  is_pdv,
  report_date,
  route,
  channel,
  group_name,
  bo_amount,
  vt_amount,
  chosen_amount
FROM loaded_refs
WHERE report_date = ?1
ORDER BY
  route,
  tracking_code`
      )
      .bind(
        date
      )
      .all();


  return {
    ok: true,
    source:
      "D1",
    date,
    rows:
      result?.results ||
      []
  };

}



async function d1AdvisorDay(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportOrdersFresh(
    env
  );


  const date =
    String(
      params.get(
        "date"
      ) ||
      ""
    )
      .trim();


  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(
      date
    )
  ) {

    throw new Error(
      "Envía date=YYYY-MM-DD."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const sql =
sellerReportEffectiveCteSql() +
`SELECT
  advisor,
  seller_original,
  source,
  effective_segment AS channel,
  effective_level AS level,
  tracking_code,
  amount
FROM effective
WHERE report_date = ?1
ORDER BY
  advisor,
  source,
  tracking_code`;


  const result =
    await env.DB
      .prepare(
        sql
      )
      .bind(
        date
      )
      .all();


  const rows =
    result?.results ||
    [];


  const totals = {};


  for (
    const row
    of rows
  ) {

    const advisor =
      row.advisor ||
      "SIN ASIGNAR";


    if (!totals[advisor]) {

      totals[advisor] = {
        total: 0,
        ZASCONNECT: 0,
        "PDV BO/VT": 0,
        PETAPA: 0,
        XENACOJ: 0,
        DIGITAL: 0
      };

    }


    totals[
      advisor
    ].total =
      round2(
        totals[
          advisor
        ].total +
        numberValue(
          row.amount
        )
      );


    if (
      totals[
        advisor
      ][
        row.source
      ] !==
      undefined
    ) {

      totals[
        advisor
      ][
        row.source
      ] =
        round2(
          totals[
            advisor
          ][
            row.source
          ] +
          numberValue(
            row.amount
          )
        );

    }


    if (
      totals[
        advisor
      ][
        row.channel
      ] !==
      undefined
    ) {

      totals[
        advisor
      ][
        row.channel
      ] =
        round2(
          totals[
            advisor
          ][
            row.channel
          ] +
          numberValue(
            row.amount
          )
        );

    }

  }


  return {
    ok: true,
    source:
      "D1",
    date,
    rows,
    totals
  };

}



async function d1PdvDay(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const date =
    String(
      params.get(
        "date"
      ) ||
      ""
    )
      .trim();


  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(
      date
    )
  ) {

    throw new Error(
      "Envía date=YYYY-MM-DD."
    );

  }


  const result =
    await env.DB
      .prepare(
`SELECT
  tracking_code,
  report_date,
  advisor,
  seller_original,
  level,
  channel,
  amount,
  source
FROM report_orders
WHERE report_date = ?1
  AND (
    source = 'PDV BO/VT'
    OR order_type = 'PDV'
  )
ORDER BY
  channel,
  advisor,
  tracking_code`
      )
      .bind(
        date
      )
      .all();


  const rows =
    result?.results ||
    [];


  return {
    ok: true,

    source:
      "D1",

    date,

    rows,

    totals: {
      PETAPA:
        round2(
          sum(
            rows
              .filter(
                row =>
                  row.channel ===
                  "PETAPA"
              )
              .map(
                row =>
                  row.amount
              )
          )
        ),

      XENACOJ:
        round2(
          sum(
            rows
              .filter(
                row =>
                  row.channel ===
                  "XENACOJ"
              )
              .map(
                row =>
                  row.amount
              )
          )
        ),

      DIGITAL:
        round2(
          sum(
            rows
              .filter(
                row =>
                  row.channel ===
                  "DIGITAL"
              )
              .map(
                row =>
                  row.amount
              )
          )
        )
    }
  };

}



async function d1DateCounts(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const from =
    params.get(
      "date_from"
    ) ||
    "1900-01-01";


  const to =
    params.get(
      "date_to"
    ) ||
    "2999-12-31";


  const result =
    await env.DB
      .prepare(
`SELECT
  report_date AS date,
  COUNT(*) AS orders,
  ROUND(SUM(amount),2) AS total
FROM report_orders
WHERE report_date
BETWEEN ?1 AND ?2
GROUP BY report_date
ORDER BY report_date`
      )
      .bind(
        from,
        to
      )
      .all();


  const summary =
    await env.DB
      .prepare(
`SELECT
  MIN(report_date) AS minDate,
  MAX(report_date) AS maxDate,
  COUNT(*) AS orders,
  COUNT(DISTINCT report_date) AS days
FROM report_orders
WHERE report_date
BETWEEN ?1 AND ?2`
      )
      .bind(
        from,
        to
      )
      .first();


  return {
    ok: true,

    source:
      "D1",

    range: {
      from,
      to
    },

    minDate:
      summary?.minDate ||
      null,

    maxDate:
      summary?.maxDate ||
      null,

    orders:
      Number(
        summary?.orders ||
        0
      ),

    days:
      Number(
        summary?.days ||
        0
      ),

    rows:
      result?.results ||
      []
  };

}



async function d1NotLoaded(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const page =
    Math.max(
      1,
      Number(
        params.get(
          "page"
        ) ||
        1
      ) ||
      1
    );


  const pageSize =
    Math.min(
      100,
      Math.max(
        1,
        Number(
          params.get(
            "page_size"
          ) ||
          100
        ) ||
        100
      )
    );


  const offset =
    (
      page -
      1
    ) *
    pageSize;


  const count =
    await env.DB
      .prepare(
`SELECT
  COUNT(*) AS total
FROM zas_orders AS z
LEFT JOIN loaded_refs AS l
  ON l.tracking_code = z.tracking_code
WHERE l.tracking_code IS NULL`
      )
      .first();


  const result =
    await env.DB
      .prepare(
`SELECT
  z.*
FROM zas_orders AS z
LEFT JOIN loaded_refs AS l
  ON l.tracking_code = z.tracking_code
WHERE l.tracking_code IS NULL
ORDER BY
  z.order_date DESC,
  z.tracking_code
LIMIT ?1
OFFSET ?2`
      )
      .bind(
        pageSize,
        offset
      )
      .all();


  const total =
    Number(
      count?.total ||
      0
    );


  return {
    ok: true,

    source:
      "D1",

    pagination: {
      page,
      pageSize,
      total,
      totalPages:
        Math.max(
          1,
          Math.ceil(
            total /
            pageSize
          )
        )
    },

    rows:
      result?.results ||
      []
  };

}




function d1BindStatement(
  statement,
  values
) {

  return statement.bind(
    ...values
  );

}



function d1BuildReportFilter(
  params,
  groupColumn =
    "group_name",
  segmentColumn =
    "order_type"
) {

  const {
    from,
    to
  } =
    d1RequireDateRange(
      params
    );


  const values = [
    from,
    to
  ];


  const conditions = [
    "report_date BETWEEN ?1 AND ?2"
  ];


  const filters = [
    [
      "group",
      groupColumn
    ],
    [
      "advisor",
      "advisor"
    ],
    [
      "type",
      segmentColumn
    ],
    [
      "source",
      "source"
    ]
  ];


  for (
    const [
      param,
      column
    ]
    of filters
  ) {

    const value =
      String(
        params.get(
          param
        ) ||
        ""
      )
        .trim();


    if (!value) {
      continue;
    }


    values.push(
      value
    );


    conditions.push(
      `${column} = ?${values.length}`
    );

  }


  return {
    from,
    to,

    where:
      conditions.join(
        "\nAND "
      ),

    values
  };

}



async function d1PublicStatus(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const [
    syncResult,
    orderResult
  ] =
    await env.DB.batch([

      env.DB.prepare(
`SELECT
  source,
  last_success_at,
  range_from,
  range_to,
  records_seen,
  records_changed,
  metadata_json,
  last_error
FROM sync_state
ORDER BY source`
      ),

      env.DB.prepare(
        "SELECT COUNT(*) AS n FROM zas_orders"
      )

    ]);


  const states =
    syncResult?.results ||
    [];


  const bySource =
    new Map(
      states.map(
        row => [
          row.source,
          row
        ]
      )
    );


  const bo =
    bySource.get(
      "BO_VT"
    ) ||
    {};


  const zas =
    bySource.get(
      "ZASCONNECT"
    ) ||
    {};


  const reports =
    bySource.get(
      "REPORTS"
    ) ||
    {};


  const scheduler =
    bySource.get(
      "SCHEDULER"
    ) ||
    {};


  let schedulerMeta = {};


  try {

    schedulerMeta =
      JSON.parse(
        scheduler.metadata_json ||
        "{}"
      );

  } catch {

    schedulerMeta = {};

  }


  let boMeta = {};


  try {

    boMeta =
      JSON.parse(
        bo.metadata_json ||
        "{}"
      );

  } catch {

    boMeta = {};

  }


  const lastSyncAt =
    [
      bo.last_success_at,
      zas.last_success_at,
      reports.last_success_at
    ]
      .filter(Boolean)
      .sort()
      .at(-1) ||
    null;


  const ageMinutes =
    lastSyncAt
      ? Math.max(
          0,
          Math.round(
            (
              Date.now() -
              new Date(
                lastSyncAt
              ).getTime()
            ) /
            60000
          )
        )
      : null;


  const schedulerLastSuccess =
    scheduler.last_success_at ||
    null;


  const schedulerAgeMinutes =
    schedulerLastSuccess
      ? Math.max(
          0,
          Math.round(
            (
              Date.now() -
              new Date(
                schedulerLastSuccess
              ).getTime()
            ) /
            60000
          )
        )
      : null;


  return {
    ok: true,

    source:
      "D1",

    googleSheets: {
      ok:
        !bo.last_error,

      lastImport:
        boMeta.lastImport ||
        bo.last_success_at ||
        null,

      sourceModified:
        boMeta.sourceModified ||
        null,

      lastSyncAt:
        bo.last_success_at ||
        null
    },

    zasConnect: {
      ok:
        !zas.last_error,

      orders:
        Number(
          (
            orderResult?.results ||
            []
          )[0]?.n ||
          0
        ),

      lastSyncAt:
        zas.last_success_at ||
        null,

      recordsChanged:
        Number(
          zas.records_changed ||
          0
        )
    },

    generatedAt:
      lastSyncAt,

    lastSyncAt,

    ageMinutes,

    stale:
      ageMinutes ===
        null ||
      ageMinutes >
        12,

    scheduler: {
      hasRun:
        Boolean(
          scheduler.last_started_at ||
          scheduler.last_success_at
        ),

      lastStartedAt:
        scheduler.last_started_at ||
        scheduler.last_success_at ||
        null,

      lastSuccessAt:
        schedulerLastSuccess,

      cron:
        schedulerMeta.cron ||
        null,

      days:
        Number(
          schedulerMeta.days ||
          0
        ),

      event:
        schedulerMeta.event ||
        null,

      lastError:
        scheduler.last_error ||
        null,

      ageMinutes:
        schedulerAgeMinutes,

      stale:
        !schedulerLastSuccess ||
        schedulerAgeMinutes >
          12
    },

    d1: {
      rangeFrom:
        reports.range_from ||
        null,

      rangeTo:
        reports.range_to ||
        null
    },

    cacheSeconds:
      0
  };

}



async function d1ReportOptions(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  await ensureReportOrdersFresh(
    env
  );


  const [
    result,
    labelInfo
  ] =
    await Promise.all([

      env.DB
        .prepare(
`SELECT
  advisor,
  source,
  report_date
FROM report_orders`
        )
        .all(),

      getReportGroupLabels(
        env
      )

    ]);


  const rows =
    result?.results ||
    [];


  const unique =
    values =>
      [
        ...new Set(
          values
            .filter(Boolean)
        )
      ]
        .sort(
          (
            a,
            b
          ) =>
            String(a)
              .localeCompare(
                String(b),
                "es"
              )
        );


  const dates =
    rows
      .map(
        row =>
          row.report_date
      )
      .filter(Boolean)
      .sort();


  return {
    ok: true,

    source:
      "D1",

    groupLabels:
      labelInfo.labels,

    options: {
      groups:
        [
          "K1",
          "K2",
          "K3"
        ]
          .map(
            level => ({
              value:
                level,

              label:
                labelInfo.labels[
                  level
                ] ||
                level
            })
          ),

      advisors:
        unique(
          rows.map(
            row =>
              row.advisor
          )
        ),

      types: [
        "DIGITAL",
        "PETAPA",
        "XENACOJ"
      ],

      sources:
        unique(
          rows.map(
            row =>
              row.source
          )
        ),

      minDate:
        dates[0] ||
        null,

      maxDate:
        dates.at(
          -1
        ) ||
        null
    }
  };

}



function d1PendingJoinedToApi(
  row
) {

  return {
    trackingCode:
      row.tracking_code,

    customerTrackingCode:
      row.customer_tracking_code,

    taxId:
      row.tax_id,

    customerName:
      row.customer_name,

    primaryPhone:
      row.primary_phone,

    secondaryPhone:
      row.secondary_phone,

    sellerName:
      row.seller_name,

    shippingMunicipality:
      row.shipping_municipality,

    shippingReference:
      row.shipping_reference,

    custom_rcf:
      row.custom_rcf,

    salesChannel:
      row.sales_channel,

    shippingDepartment:
      row.shipping_department,

    orderDate:
      row.order_date,

    sku:
      row.line_sku,

    description:
      row.line_description,

    quantity:
      row.line_quantity,

    brand:
      row.line_brand,

    locationCode:
      row.line_location_code,

    ringCode:
      row.line_ring_code,

    comments:
      row.comments,

    warehouse:
      row.warehouse,

    orderTotal:
      row.order_total,

    unitPrice:
      row.line_unit_price,

    lineNumber:
      row.line_number,

    _lineKey:
      row.line_key ??
      null,

    shippingAvenue:
      row.shipping_avenue,

    shippingHouseNumber:
      row.shipping_house_number,

    shippingZone:
      row.shipping_zone,

    lineTotal:
      row.line_total,

    custom_bodega:
      row.custom_bodega,

    custom_canal:
      row.custom_canal,

    custom_payment_method:
      row.custom_payment_method,

    custom_marca:
      row.custom_marca
  };

}




let pendingNameRulesSchemaPromise =
  null;


function buildStaticPendingNameSeedRows() {

  const grouped =
    new Map();


  for (
    const entry
    of PENDING_NAME_NORMALIZATION
  ) {

    const canonical =
      String(
        entry?.canonical ??
        ""
      )
        .trim();


    if (!canonical) {
      continue;
    }


    const key =
      normalizeMatchText(
        canonical
      );


    if (
      !grouped.has(
        key
      )
    ) {

      grouped.set(
        key,
        {
          canonical,
          aliases: []
        }
      );

    }


    const target =
      grouped.get(
        key
      );


    for (
      const alias
      of (
        entry.aliases ||
        []
      )
    ) {

      const value =
        String(
          alias ??
          ""
        )
          .trim();


      if (
        !value ||
        normalizeMatchText(
          value
        ) ===
        normalizeMatchText(
          canonical
        )
      ) {
        continue;
      }


      if (
        !target.aliases.some(
          existing =>
            normalizeMatchText(
              existing
            ) ===
            normalizeMatchText(
              value
            )
        )
      ) {

        target.aliases.push(
          value
        );

      }

    }

  }


  const rows = [];


  for (
    const group
    of grouped.values()
  ) {

    if (
      !group.aliases.length
    ) {

      rows.push({
        canonical:
          group.canonical,
        aliases: []
      });


      continue;

    }


    for (
      let i = 0;
      i <
      group.aliases.length;
      i += 5
    ) {

      rows.push({
        canonical:
          group.canonical,
        aliases:
          group.aliases.slice(
            i,
            i + 5
          )
      });

    }

  }


  return rows;

}



async function ensurePendingNameRulesSchema(
  env
) {

  if (
    pendingNameRulesSchemaPromise
  ) {

    return pendingNameRulesSchemaPromise;

  }


  pendingNameRulesSchemaPromise =
    (
      async () => {

        await env.DB
          .prepare(
`CREATE TABLE IF NOT EXISTS pending_name_normalization (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_name TEXT NOT NULL,
  alias1 TEXT,
  alias2 TEXT,
  alias3 TEXT,
  alias4 TEXT,
  alias5 TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)`
          )
          .run();


        const state =
          await env.DB
            .prepare(
`SELECT
  source
FROM sync_state
WHERE source = 'NAME_NORMALIZATION'`
            )
            .first();


        if (
          !state
        ) {

          const seedRows =
            buildStaticPendingNameSeedRows();


          const now =
            new Date()
              .toISOString();


          const statements =
            seedRows.map(
              row => {

                const aliases =
                  [
                    ...row.aliases,
                    "",
                    "",
                    "",
                    "",
                    ""
                  ]
                    .slice(
                      0,
                      5
                    );


                return env.DB
                  .prepare(
`INSERT INTO pending_name_normalization (
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5,
  created_at,
  updated_at
)
VALUES (
  ?1,?2,?3,?4,?5,?6,?7,?7
)`
                  )
                  .bind(
                    row.canonical,
                    aliases[0] ||
                      null,
                    aliases[1] ||
                      null,
                    aliases[2] ||
                      null,
                    aliases[3] ||
                      null,
                    aliases[4] ||
                      null,
                    now
                  );

              }
            );


          for (
            let i = 0;
            i <
            statements.length;
            i += 40
          ) {

            await env.DB.batch(
              statements.slice(
                i,
                i + 40
              )
            );

          }


          await env.DB
            .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'NAME_NORMALIZATION',
  ?1,
  ?2,
  ?2,
  ?3
)`
            )
            .bind(
              now,
              seedRows.length,
              JSON.stringify({
                seeded:
                  true,
                maxAliasesPerRow:
                  5
              })
            )
            .run();

        }

      }
    )()
      .catch(
        error => {

          pendingNameRulesSchemaPromise =
            null;


          throw error;

        }
      );


  return pendingNameRulesSchemaPromise;

}



function pendingNameRowToRule(
  row
) {

  const aliases =
    [
      row.alias1,
      row.alias2,
      row.alias3,
      row.alias4,
      row.alias5
    ]
      .map(
        value =>
          String(
            value ??
            ""
          )
            .trim()
      )
      .filter(Boolean);


  return {
    id:
      Number(
        row.id
      ),

    canonical:
      String(
        row.canonical_name ??
        ""
      )
        .trim(),

    aliases,

    kLevel:
      row.k_level ||
      null,

    segment:
      row.segment ||
      null
  };

}



async function getPendingNameRules(
  env
) {

  await ensureReportRulesSchema(
    env
  );


  const result =
    await env.DB
      .prepare(
`SELECT
  n.id,
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  c.k_level,
  c.segment

FROM pending_name_normalization AS n

LEFT JOIN seller_report_rule_rows AS c
  ON c.normalization_id =
     n.id

ORDER BY
  n.canonical_name COLLATE NOCASE,
  n.id`
      )
      .all();


  return (
    result?.results ||
    []
  )
    .map(
      pendingNameRowToRule
    );

}



async function pendingNameRulesList(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const [
    rulesResult,
    sellersResult
  ] =
    await env.DB.batch([

      env.DB.prepare(
`SELECT
  n.id,
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  n.updated_at,
  c.k_level,
  c.segment

FROM pending_name_normalization AS n

LEFT JOIN seller_report_rule_rows AS c
  ON c.normalization_id =
     n.id

ORDER BY
  n.canonical_name COLLATE NOCASE,
  n.id`
      ),

      env.DB.prepare(
`SELECT
  seller_name
FROM zas_orders
WHERE seller_name IS NOT NULL
  AND TRIM(seller_name) <> ''
GROUP BY seller_name
ORDER BY seller_name COLLATE NOCASE`
      )

    ]);


  const rules =
    (
      rulesResult?.results ||
      []
    )
      .map(
        row => ({
          ...pendingNameRowToRule(
            row
          ),
          updatedAt:
            row.updated_at ||
            null
        })
      );


  return {
    ok: true,

    source:
      "D1",

    rules,

    rawSellers:
      (
        sellersResult?.results ||
        []
      )
        .map(
          row =>
            row.seller_name
        )
        .filter(Boolean),

    maxAliases:
      5,

    kLevels: [
      "K1",
      "K2",
      "K3"
    ],

    segments: [
      "DIGITAL",
      "PETAPA",
      "XENACOJ"
    ],

    reportLogic: {
      apiDefault:
        "DIGITAL",
      pdvBoVt:
        "TIENDA_FISICA",
      physicalSegments: [
        "PETAPA",
        "XENACOJ"
      ]
    }
  };

}



async function pendingNameRuleSave(
  env,
  body
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const id =
    Number(
      body?.id ||
      0
    );


  const canonical =
    String(
      body?.canonical ??
      ""
    )
      .trim();


  const aliases =
    (
      Array.isArray(
        body?.aliases
      )
        ? body.aliases
        : []
    )
      .map(
        value =>
          String(
            value ??
            ""
          )
            .trim()
      )
      .filter(Boolean)
      .filter(
        (
          value,
          index,
          values
        ) =>
          values.findIndex(
            candidate =>
              normalizeMatchText(
                candidate
              ) ===
              normalizeMatchText(
                value
              )
          ) ===
          index
      )
      .filter(
        value =>
          normalizeMatchText(
            value
          ) !==
          normalizeMatchText(
            canonical
          )
      )
      .slice(
        0,
        5
      );


  if (!canonical) {

    throw new Error(
      "Escribe el nombre final de la normalización."
    );

  }


  const padded =
    [
      ...aliases,
      "",
      "",
      "",
      "",
      ""
    ]
      .slice(
        0,
        5
      );


  const kLevel =
    validReportK(
      body?.kLevel
    );


  const segment =
    validReportSegment(
      body?.segment
    );


  const now =
    new Date()
      .toISOString();


  let savedId =
    id;


  if (
    id >
    0
  ) {

    const existing =
      await env.DB
        .prepare(
`SELECT id
FROM pending_name_normalization
WHERE id = ?1`
        )
        .bind(
          id
        )
        .first();


    if (!existing) {

      throw new Error(
        "La normalización ya no existe. Recarga la tabla."
      );

    }


    await env.DB
      .prepare(
`UPDATE pending_name_normalization
SET
  canonical_name = ?2,
  alias1 = ?3,
  alias2 = ?4,
  alias3 = ?5,
  alias4 = ?6,
  alias5 = ?7,
  updated_at = ?8
WHERE id = ?1`
      )
      .bind(
        id,
        canonical,
        padded[0] ||
          null,
        padded[1] ||
          null,
        padded[2] ||
          null,
        padded[3] ||
          null,
        padded[4] ||
          null,
        now
      )
      .run();


  } else {

    const result =
      await env.DB
        .prepare(
`INSERT INTO pending_name_normalization (
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5,
  created_at,
  updated_at
)
VALUES (
  ?1,?2,?3,?4,?5,?6,?7,?7
)
RETURNING id`
        )
        .bind(
          canonical,
          padded[0] ||
            null,
          padded[1] ||
            null,
          padded[2] ||
            null,
          padded[3] ||
            null,
          padded[4] ||
            null,
          now
        )
        .first();


    savedId =
      Number(
        result?.id ||
        0
      );

  }


  const reportRule =
    await saveSellerReportRuleForNormalization(
      env,
      savedId,
      kLevel,
      segment
    );


  await env.DB
    .prepare(
`UPDATE sync_state
SET
  last_success_at = ?1,
  records_changed = records_changed + 1,
  metadata_json = ?2
WHERE source = 'NAME_NORMALIZATION'`
    )
    .bind(
      now,
      JSON.stringify({
        lastAction:
          "SAVE",
        maxAliasesPerRow:
          5
      })
    )
    .run();


  return {
    ok: true,

    id:
      savedId,

    canonical,

    aliases,

    kLevel:
      reportRule.kLevel,

    segment:
      reportRule.segment,

    message:
      "Normalización y clasificación guardadas."
  };

}



async function pendingNameRuleDelete(
  env,
  idValue
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const id =
    Number(
      idValue ||
      0
    );


  if (
    !Number.isInteger(
      id
    ) ||
    id <=
      0
  ) {

    throw new Error(
      "ID de normalización inválido."
    );

  }


  await env.DB.batch([

    env.DB.prepare(
`DELETE FROM seller_report_match
WHERE normalization_id = ?1`
    )
      .bind(
        id
      ),

    env.DB.prepare(
`DELETE FROM seller_report_rule_rows
WHERE normalization_id = ?1`
    )
      .bind(
        id
      )

  ]);


  const result =
    await env.DB
      .prepare(
`DELETE FROM pending_name_normalization
WHERE id = ?1`
      )
      .bind(
        id
      )
      .run();


  await rebuildSellerReportMatch(
    env
  );


  const now =
    new Date()
      .toISOString();


  await env.DB
    .prepare(
`UPDATE sync_state
SET
  last_success_at = ?1,
  records_changed = records_changed + 1,
  metadata_json = ?2
WHERE source = 'NAME_NORMALIZATION'`
    )
    .bind(
      now,
      JSON.stringify({
        lastAction:
          "DELETE",
        maxAliasesPerRow:
          5
      })
    )
    .run();


  return {
    ok: true,

    id,

    deleted:
      Number(
        result?.meta?.changes ||
        0
      ) >
      0,

    message:
      "Normalización eliminada."
  };

}




let reportRulesSchemaPromise =
  null;


const SELLER_REPORT_SEED_V1 = [

  {
    canonical:
      "KAREN PAOLA SOLÍS",
    match: [
      "KAREN PAOLA SOLÍS",
      "KAREN PAOLA SOLIS"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "YESSICA CARRERA",
    match: [
      "YESICA CARRERA",
      "YESSICA CARRERA",
      "YESSICA ALEJANDRA CARRERA PINEDA"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "YOSELIN BARRAZA",
    match: [
      "YOSELIN EUFEMIA BARRAZA ARIAS",
      "YOSELIN BARRAZA"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "FABIOLA RODAS",
    match: [
      "FABIOLA RODAS"
    ],
    kLevel:
      "K3",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "VILMA PEÑA",
    match: [
      "VILMA PEÑA",
      "VILMA GLORIBEL PEÑA"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "ALEJANDRA SIAN",
    match: [
      "ENMA ALEJANDRA SIAN COROMINAL",
      "ALEJANDRA SIAN"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "HENRY PACHECO",
    match: [
      "HENRY ESTUARDO PACHECO ARIANO",
      "HENRY PACHECO"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "MARIA SANTACRUZ",
    match: [
      "MARIA RENEE SANTA CRUZ",
      "MARIA RENE SANTA CRUZ COSAJAY",
      "MARIA SANTACRUZ"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "ERICK SIQUIBACHE",
    match: [
      "ERICK FERNANDO SIQUIBACHE SOLORZANO",
      "ERICK SIQUIBACHE"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "FELIX ACEITUNO",
    match: [
      "FELIX ACEITUNO",
      "FELIX ANTONIO ACEITUNO BARRIENTOS"
    ],
    kLevel:
      "K1",
    segment:
      "PETAPA"
  },

  {
    canonical:
      "VICENTE PEREZ",
    match: [
      "VICENTE PEREZ",
      "VICENTE PEREZ ARGUETA"
    ],
    kLevel:
      "K1",
    segment:
      "PETAPA"
  },

  {
    canonical:
      "LADY PAREDES",
    match: [
      "LADY PAREDES"
    ],
    kLevel:
      "K3",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "JUAN MEJIA",
    match: [
      "JUAN MEJIA"
    ],
    kLevel:
      "K3",
    segment:
      "PETAPA"
  },

  {
    canonical:
      "WALTER GUERRA",
    match: [
      "WALTER GUERRA",
      "WALTER NEHEMIAS GUERRA"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "JOSSELYN ROXANA SANTIAGO",
    match: [
      "JOSSELIN ROXANA SANTIAGO",
      "JOSSELYN ROXANA SANTIAGO"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "LIDIA PEREZ",
    match: [
      "LIDIA PEREZ",
      "LIDIA PÉREZ"
    ],
    kLevel:
      "K3",
    segment:
      "DIGITAL"
  },

  {
    canonical:
      "JHONATAN BARRIOS",
    match: [
      "JHONATAN ISRAEL BARRIOS MUÑOZ",
      "JHONATAN BARRIOS"
    ],
    kLevel:
      "K2",
    segment:
      "DIGITAL"
  },

  // Se conservan reglas históricas de piso.
  {
    canonical:
      "JUAN COTZAJAY",
    match: [
      "JUAN COTZAJAY",
      "JUAN LUIS COTZAJAY TIJE"
    ],
    kLevel:
      "K1",
    segment:
      "PETAPA"
  },
  {
    canonical:
      "MARVIN CANEL",
    match: [
      "MARVIN CANEL",
      "MARVIN DAVID CANEL HERNANDEZ"
    ],
    kLevel:
      "K1",
    segment:
      "XENACOJ"
  }

];



function validReportK(
  value
) {

  const text =
    String(
      value ??
      ""
    )
      .trim()
      .toUpperCase();


  return [
    "K1",
    "K2",
    "K3"
  ]
    .includes(
      text
    )
      ? text
      : null;

}



function validReportSegment(
  value
) {

  const text =
    String(
      value ??
      ""
    )
      .trim()
      .toUpperCase();


  return [
    "DIGITAL",
    "PETAPA",
    "XENACOJ"
  ]
    .includes(
      text
    )
      ? text
      : null;

}




async function ensureJuanGlendaSeparated(
  env
) {

  // IMPORTANTE:
  // La normalización de nombre sirve para ÓRDENES PENDIENTES.
  // En reportes NO renombramos vendedores al canonical.
  //
  // Por ejemplo, Juan Mejia puede estar como alias dentro de una fila
  // canónica "Glenda Reinoso" para pendientes, pero en reportes debe
  // seguir mostrándose como JUAN MEJIA y únicamente heredar K/Segmento
  // de la fila que haga match.
  //
  // Por eso esta migración ya NO modifica pending_name_normalization
  // ni fuerza K/Segmento de Juan o Glenda.

  const migration =
    await env.DB
      .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_IDENTITY_RULES_V2'`
      )
      .first();


  if (migration) {
    return false;
  }


  await rebuildSellerReportMatch(
    env
  );


  const now =
    new Date()
      .toISOString();


  await env.DB
    .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_IDENTITY_RULES_V2',
  ?1,
  1,
  1,
  ?2
)`
    )
    .bind(
      now,
      JSON.stringify({
        pendingCanonicalization:
          true,
        reportAdvisorIdentityPreserved:
          true,
        reportUsesNormalizationForClassificationOnly:
          true
      })
    )
    .run();


  return true;

}



async function ensureReportRulesSchema(
  env
) {

  if (
    reportRulesSchemaPromise
  ) {

    return reportRulesSchemaPromise;

  }


  reportRulesSchemaPromise =
    (
      async () => {

        await ensurePendingNameRulesSchema(
          env
        );


        await env.DB.batch([

          env.DB.prepare(
`CREATE TABLE IF NOT EXISTS report_group_labels (
  level TEXT PRIMARY KEY CHECK (
    level IN ('K1','K2','K3')
  ),
  display_name TEXT NOT NULL,
  sort_order INTEGER NOT NULL,
  updated_at TEXT NOT NULL
)`
          ),

          env.DB.prepare(
`CREATE TABLE IF NOT EXISTS seller_report_rule_rows (
  normalization_id INTEGER PRIMARY KEY,
  k_level TEXT CHECK (
    k_level IS NULL OR
    k_level IN ('K1','K2','K3')
  ),
  segment TEXT CHECK (
    segment IS NULL OR
    segment IN ('DIGITAL','PETAPA','XENACOJ')
  ),
  updated_at TEXT NOT NULL
)`
          ),

          env.DB.prepare(
`CREATE TABLE IF NOT EXISTS seller_report_match (
  match_key TEXT PRIMARY KEY,
  normalization_id INTEGER NOT NULL,
  canonical_name TEXT NOT NULL,
  k_level TEXT,
  segment TEXT,
  updated_at TEXT NOT NULL
)`
          ),

          env.DB.prepare(
`CREATE INDEX IF NOT EXISTS idx_seller_report_match_norm
ON seller_report_match(normalization_id)`
          ),

          env.DB.prepare(
`CREATE TABLE IF NOT EXISTS seller_monthly_targets (
  seller_key TEXT NOT NULL,
  seller_name TEXT NOT NULL,
  month TEXT NOT NULL,
  target_amount REAL NOT NULL DEFAULT 0 CHECK (target_amount >= 0),
  updated_at TEXT NOT NULL,
  PRIMARY KEY (seller_key, month)
)`
          ),

          env.DB.prepare(
`CREATE INDEX IF NOT EXISTS idx_seller_monthly_targets_month
ON seller_monthly_targets(month)`
          ),

          env.DB.prepare(
`INSERT INTO report_group_labels (
  level,
  display_name,
  sort_order,
  updated_at
)
VALUES (
  'K1','K1',1,datetime('now')
)
ON CONFLICT(level) DO NOTHING`
          ),

          env.DB.prepare(
`INSERT INTO report_group_labels (
  level,
  display_name,
  sort_order,
  updated_at
)
VALUES (
  'K2','K2',2,datetime('now')
)
ON CONFLICT(level) DO NOTHING`
          ),

          env.DB.prepare(
`INSERT INTO report_group_labels (
  level,
  display_name,
  sort_order,
  updated_at
)
VALUES (
  'K3','K3',3,datetime('now')
)
ON CONFLICT(level) DO NOTHING`
          )

        ]);


        const seedState =
          await env.DB
            .prepare(
`SELECT source
FROM sync_state
WHERE source = 'SELLER_REPORT_RULES_SEED_V1'`
            )
            .first();


        if (!seedState) {

          const rowsResult =
            await env.DB
              .prepare(
`SELECT
  id,
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5
FROM pending_name_normalization
ORDER BY id`
              )
              .all();


          let rows =
            rowsResult?.results ||
            [];


          function rowCandidates(
            row
          ) {

            return [
              row.canonical_name,
              row.alias1,
              row.alias2,
              row.alias3,
              row.alias4,
              row.alias5
            ]
              .map(
                normText
              )
              .filter(Boolean);

          }


          for (
            const seed
            of SELLER_REPORT_SEED_V1
          ) {

            const wanted =
              seed.match
                .map(
                  normText
                )
                .filter(Boolean);


            let found =
              rows.find(
                row => {

                  const candidates =
                    rowCandidates(
                      row
                    );


                  return wanted.some(
                    value =>
                      candidates.includes(
                        value
                      )
                  );

                }
              );


            if (!found) {

              // Segundo intento: canonical exacto.
              const canonicalKey =
                normText(
                  seed.canonical
                );


              found =
                rows.find(
                  row =>
                    normText(
                      row.canonical_name
                    ) ===
                    canonicalKey
                );

            }


            if (!found) {

              const aliases =
                seed.match
                  .filter(
                    value =>
                      normText(
                        value
                      ) !==
                      normText(
                        seed.canonical
                      )
                  )
                  .slice(
                    0,
                    5
                  );


              const padded =
                [
                  ...aliases,
                  null,
                  null,
                  null,
                  null,
                  null
                ]
                  .slice(
                    0,
                    5
                  );


              const now =
                new Date()
                  .toISOString();


              const inserted =
                await env.DB
                  .prepare(
`INSERT INTO pending_name_normalization (
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5,
  created_at,
  updated_at
)
VALUES (
  ?1,?2,?3,?4,?5,?6,?7,?7
)
RETURNING
  id,
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5`
                  )
                  .bind(
                    seed.canonical,
                    padded[0],
                    padded[1],
                    padded[2],
                    padded[3],
                    padded[4],
                    now
                  )
                  .first();


              found =
                inserted;


              rows.push(
                inserted
              );

            }


            const now =
              new Date()
                .toISOString();


            await env.DB
              .prepare(
`INSERT INTO seller_report_rule_rows (
  normalization_id,
  k_level,
  segment,
  updated_at
)
VALUES (
  ?1,?2,?3,?4
)
ON CONFLICT(normalization_id)
DO UPDATE SET
  k_level = CASE
    WHEN seller_report_rule_rows.k_level IS NULL
    THEN excluded.k_level
    ELSE seller_report_rule_rows.k_level
  END,
  segment = CASE
    WHEN seller_report_rule_rows.segment IS NULL
    THEN excluded.segment
    ELSE seller_report_rule_rows.segment
  END,
  updated_at = excluded.updated_at`
              )
              .bind(
                Number(
                  found.id
                ),
                seed.kLevel,
                seed.segment,
                now
              )
              .run();

          }


          await rebuildSellerReportMatch(
            env
          );


          const now =
            new Date()
              .toISOString();


          await env.DB
            .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'SELLER_REPORT_RULES_SEED_V1',
  ?1,
  ?2,
  ?2,
  ?3
)`
            )
            .bind(
              now,
              SELLER_REPORT_SEED_V1.length,
              JSON.stringify({
                source:
                  "user_table_2026_09_30_to_2026_10_10",
                seeded:
                  true
              })
            )
            .run();

        } else {

          const matchCount =
            await env.DB
              .prepare(
`SELECT COUNT(*) AS n
FROM seller_report_match`
              )
              .first();


          if (
            Number(
              matchCount?.n ||
              0
            ) ===
            0
          ) {

            await rebuildSellerReportMatch(
              env
            );

          }

        }


        // Migración única: Juan y Glenda deben permanecer separados.
        await ensureJuanGlendaSeparated(
          env
        );

      }
    )()
      .catch(
        error => {

          reportRulesSchemaPromise =
            null;


          throw error;

        }
      );


  return reportRulesSchemaPromise;

}



async function rebuildSellerReportMatch(
  env
) {

  const result =
    await env.DB
      .prepare(
`SELECT
  n.id,
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  c.k_level,
  c.segment,
  c.updated_at

FROM pending_name_normalization AS n

JOIN seller_report_rule_rows AS c
  ON c.normalization_id =
     n.id

WHERE c.k_level IS NOT NULL
   OR c.segment IS NOT NULL

ORDER BY n.id`
      )
      .all();


  const rows =
    result?.results ||
    [];


  const canonicalCounts =
    new Map();


  for (
    const row
    of rows
  ) {

    const key =
      normText(
        row.canonical_name
      );


    canonicalCounts.set(
      key,
      (
        canonicalCounts.get(
          key
        ) ||
        0
      ) +
      1
    );

  }


  const canonicalOwner =
    new Map();


  for (
    const row
    of rows
  ) {

    const key =
      normText(
        row.canonical_name
      );


    if (
      canonicalCounts.get(
        key
      ) ===
      1
    ) {

      canonicalOwner.set(
        key,
        Number(
          row.id
        )
      );


      continue;

    }


    const nonCanonicalAliases =
      [
        row.alias1,
        row.alias2,
        row.alias3,
        row.alias4,
        row.alias5
      ]
        .map(
          normText
        )
        .filter(
          value =>
            value &&
            value !==
            key
        );


    if (
      !canonicalOwner.has(
        key
      ) &&
      nonCanonicalAliases.length ===
        0
    ) {

      canonicalOwner.set(
        key,
        Number(
          row.id
        )
      );

    }

  }


  // Si un canonical duplicado no tiene una fila "base",
  // la primera fila es dueña del canonical.
  for (
    const row
    of rows
  ) {

    const key =
      normText(
        row.canonical_name
      );


    if (
      !canonicalOwner.has(
        key
      )
    ) {

      canonicalOwner.set(
        key,
        Number(
          row.id
        )
      );

    }

  }


  const matches =
    new Map();


  for (
    const row
    of rows
  ) {

    const canonicalKey =
      normText(
        row.canonical_name
      );


    const candidates = [];


    if (
      canonicalOwner.get(
        canonicalKey
      ) ===
      Number(
        row.id
      )
    ) {

      candidates.push(
        row.canonical_name
      );

    }


    candidates.push(
      row.alias1,
      row.alias2,
      row.alias3,
      row.alias4,
      row.alias5
    );


    for (
      const value
      of candidates
    ) {

      const key =
        normText(
          value
        );


      if (!key) {
        continue;
      }


      matches.set(
        key,
        {
          match_key:
            key,

          normalization_id:
            Number(
              row.id
            ),

          canonical_name:
            row.canonical_name,

          k_level:
            row.k_level ||
            null,

          segment:
            row.segment ||
            null,

          updated_at:
            row.updated_at ||
            new Date()
              .toISOString()
        }
      );

    }

  }


  await env.DB
    .prepare(
      "DELETE FROM seller_report_match"
    )
    .run();


  const items =
    [
      ...matches.values()
    ];


  for (
    let i = 0;
    i <
    items.length;
    i += 40
  ) {

    const chunk =
      items.slice(
        i,
        i + 40
      );


    if (
      !chunk.length
    ) {
      continue;
    }


    await env.DB.batch(
      chunk.map(
        item =>
          env.DB
            .prepare(
`INSERT INTO seller_report_match (
  match_key,
  normalization_id,
  canonical_name,
  k_level,
  segment,
  updated_at
)
VALUES (
  ?1,?2,?3,?4,?5,?6
)
ON CONFLICT(match_key)
DO UPDATE SET
  normalization_id = excluded.normalization_id,
  canonical_name = excluded.canonical_name,
  k_level = excluded.k_level,
  segment = excluded.segment,
  updated_at = excluded.updated_at`
            )
            .bind(
              item.match_key,
              item.normalization_id,
              item.canonical_name,
              item.k_level,
              item.segment,
              item.updated_at
            )
      )
    );

  }


  return {
    count:
      items.length
  };

}



async function saveSellerReportRuleForNormalization(
  env,
  normalizationId,
  kLevelValue,
  segmentValue
) {

  await ensureReportRulesSchema(
    env
  );


  const normalizationIdNumber =
    Number(
      normalizationId
    );


  const kLevel =
    validReportK(
      kLevelValue
    );


  const segment =
    validReportSegment(
      segmentValue
    );


  if (
    !kLevel &&
    !segment
  ) {

    await env.DB
      .prepare(
`DELETE FROM seller_report_rule_rows
WHERE normalization_id = ?1`
      )
      .bind(
        normalizationIdNumber
      )
      .run();

  } else {

    const now =
      new Date()
        .toISOString();


    await env.DB
      .prepare(
`INSERT INTO seller_report_rule_rows (
  normalization_id,
  k_level,
  segment,
  updated_at
)
VALUES (
  ?1,?2,?3,?4
)
ON CONFLICT(normalization_id)
DO UPDATE SET
  k_level = excluded.k_level,
  segment = excluded.segment,
  updated_at = excluded.updated_at`
      )
      .bind(
        normalizationIdNumber,
        kLevel,
        segment,
        now
      )
      .run();

  }


  await rebuildSellerReportMatch(
    env
  );


  return {
    kLevel,
    segment
  };

}



async function getReportGroupLabels(
  env
) {

  await ensureReportRulesSchema(
    env
  );


  const result =
    await env.DB
      .prepare(
`SELECT
  level,
  display_name,
  sort_order
FROM report_group_labels
ORDER BY sort_order, level`
      )
      .all();


  const labels = {
    K1:
      "K1",
    K2:
      "K2",
    K3:
      "K3"
  };


  for (
    const row
    of (
      result?.results ||
      []
    )
  ) {

    if (
      labels[
        row.level
      ] !==
      undefined
    ) {

      labels[
        row.level
      ] =
        String(
          row.display_name ||
          row.level
        );

    }

  }


  return {
    labels,
    rows:
      result?.results ||
      []
  };

}



async function reportConfigGet(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const info =
    await getReportGroupLabels(
      env
    );


  return {
    ok: true,

    source:
      "D1",

    labels:
      info.labels,

    note:
      "Los nombres visuales no modifican el código interno K1/K2/K3. K y segmento por vendedor se configuran en ⚙ NOMBRES."
  };

}



async function reportConfigSaveLabels(
  env,
  body
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureReportRulesSchema(
    env
  );


  const incoming =
    body?.labels ||
    {};


  const values = {
    K1:
      String(
        incoming.K1 ??
        ""
      )
        .trim(),

    K2:
      String(
        incoming.K2 ??
        ""
      )
        .trim(),

    K3:
      String(
        incoming.K3 ??
        ""
      )
        .trim()
  };


  for (
    const level
    of [
      "K1",
      "K2",
      "K3"
    ]
  ) {

    if (
      !values[
        level
      ]
    ) {

      throw new Error(
        "Los nombres visuales K1, K2 y K3 son obligatorios."
      );

    }

  }


  const now =
    new Date()
      .toISOString();


  await env.DB.batch(
    [
      "K1",
      "K2",
      "K3"
    ]
      .map(
        (
          level,
          index
        ) =>
          env.DB
            .prepare(
`INSERT INTO report_group_labels (
  level,
  display_name,
  sort_order,
  updated_at
)
VALUES (
  ?1,?2,?3,?4
)
ON CONFLICT(level)
DO UPDATE SET
  display_name = excluded.display_name,
  sort_order = excluded.sort_order,
  updated_at = excluded.updated_at`
            )
            .bind(
              level,
              values[
                level
              ],
              index +
              1,
              now
            )
      )
  );


  return {
    ok: true,

    labels:
      values,

    message:
      "Nombres visuales guardados."
  };

}



function sellerReportEffectiveCteSql() {

  return `
WITH effective AS (
  SELECT
    r.*,

    CASE
      -- Tracking/API: K viene de la regla del vendedor.
      -- Si no hay regla manual, conservar el K base del API.
      WHEN r.source = 'ZASCONNECT'
      THEN COALESCE(
        srm.k_level,
        r.level
      )

      -- BO/VT sin U_IDFRONT: puede usar regla de vendedor si existe.
      ELSE COALESCE(
        srm.k_level,
        r.level
      )
    END AS effective_level,

    CASE

      -- REGLA PRINCIPAL:
      -- Si existe tracking en ZasConnect, el segmento se toma del
      -- vendedor real del API según ⚙ NOMBRES.
      --
      -- BO/VT NO puede convertir Fabiola a PETAPA solo porque Ruta=Glenda.
      -- BO/VT tampoco puede cambiar Juan/otro vendedor a otro segmento.
      WHEN r.source = 'ZASCONNECT'
      THEN
        CASE
          WHEN srm.segment IN (
            'DIGITAL',
            'PETAPA',
            'XENACOJ'
          )
          THEN srm.segment

          ELSE 'DIGITAL'
        END

      -- Venta exclusivamente BO/VT (normalmente PDV-NOID por UID vacío):
      -- aquí sí manda el dato físico real.
      WHEN r.source = 'PDV BO/VT'
      THEN
        CASE
          WHEN lr.channel IN (
            'PETAPA',
            'XENACOJ'
          )
          THEN lr.channel

          WHEN r.channel IN (
            'PETAPA',
            'XENACOJ'
          )
          THEN r.channel

          WHEN srm.segment IN (
            'DIGITAL',
            'PETAPA',
            'XENACOJ'
          )
          THEN srm.segment

          ELSE 'PETAPA'
        END

      -- Respaldo para cualquier fuente futura.
      WHEN srm.segment IN (
        'DIGITAL',
        'PETAPA',
        'XENACOJ'
      )
      THEN srm.segment

      ELSE 'DIGITAL'

    END AS effective_segment

  FROM report_orders AS r

  LEFT JOIN loaded_refs AS lr
    ON lr.tracking_code =
       r.tracking_code

  LEFT JOIN seller_report_match AS srm
    ON srm.match_key =
       UPPER(
         TRIM(
           COALESCE(
             r.seller_original,
             r.advisor,
             ''
           )
         )
       )
)
`;
}



async function d1NotLoadedDashboard(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureManualSapSchema(
    env
  );


  // Cabeceras y líneas se consultan por separado.
  // Así una orden pendiente con 5 filas reales en zas_order_lines
  // siempre produce 5 filas visibles en Órdenes no cargadas.

  const [
    ordersResult,
    linesResult
  ] =
    await env.DB.batch([

      env.DB.prepare(
`SELECT
  z.*

FROM zas_orders AS z

LEFT JOIN loaded_refs AS r
  ON r.tracking_code =
     z.tracking_code

LEFT JOIN manual_sap_control AS m
  ON m.tracking_code =
     z.tracking_code

WHERE r.tracking_code IS NULL
  AND m.tracking_code IS NULL

ORDER BY
  z.order_date DESC,
  z.tracking_code`
      ),

      env.DB.prepare(
`SELECT
  l.tracking_code,
  l.line_key,
  l.line_number,
  l.sku AS line_sku,
  l.description AS line_description,
  l.quantity AS line_quantity,
  l.brand AS line_brand,
  l.location_code AS line_location_code,
  l.ring_code AS line_ring_code,
  l.unit_price AS line_unit_price,
  l.line_total AS line_total

FROM zas_order_lines AS l

JOIN zas_orders AS z
  ON z.tracking_code =
     l.tracking_code

LEFT JOIN loaded_refs AS r
  ON r.tracking_code =
     z.tracking_code

LEFT JOIN manual_sap_control AS m
  ON m.tracking_code =
     z.tracking_code

WHERE r.tracking_code IS NULL
  AND m.tracking_code IS NULL

ORDER BY
  z.order_date DESC,
  l.tracking_code,
  CASE
    WHEN l.line_number IS NULL
      THEN 1
    ELSE 0
  END,
  l.line_number,
  l.line_key`
      )

    ]);


  const orderRows =
    ordersResult?.results ||
    [];


  const lineRows =
    linesResult?.results ||
    [];


  const linesByTracking =
    new Map();


  for (
    const line
    of lineRows
  ) {

    const tracking =
      normId(
        line.tracking_code
      );


    if (!tracking) {
      continue;
    }


    if (
      !linesByTracking.has(
        tracking
      )
    ) {

      linesByTracking.set(
        tracking,
        []
      );

    }


    linesByTracking
      .get(
        tracking
      )
      .push(
        line
      );

  }


  const apiRows = [];


  for (
    const order
    of orderRows
  ) {

    const tracking =
      normId(
        order.tracking_code
      );


    const lines =
      linesByTracking.get(
        tracking
      ) ||
      [];


    if (!lines.length) {

      apiRows.push(
        d1PendingJoinedToApi(
          {
            ...order,

            line_key:
              null,

            line_number:
              null,

            line_sku:
              null,

            line_description:
              null,

            line_quantity:
              null,

            line_brand:
              null,

            line_location_code:
              null,

            line_ring_code:
              null,

            line_unit_price:
              null,

            line_total:
              null
          }
        )
      );


      continue;

    }


    for (
      const line
      of lines
    ) {

      apiRows.push(
        d1PendingJoinedToApi(
          {
            ...order,
            ...line
          }
        )
      );

    }

  }


  const nameRules =
    await getPendingNameRules(
      env
    );


  const built =
    buildNotLoaded(
      apiRows,
      [],
      nameRules
    );


  return {
    ok:
      true,

    source:
      "D1",

    rows:
      built.rows,

    columns:
      NOT_LOADED_COLUMNS,

    kpis:
      built.kpis,

    options:
      buildNotLoadedOptions(
        built.rows
      ),

    diagnostics: {
      pendingOrderHeaders:
        orderRows.length,

      pendingProductLines:
        lineRows.length,

      renderedRows:
        built.rows.length
    },

    updatedAt:
      new Date()
        .toISOString()
  };

}


let manualSapSchemaPromise = null;


async function ensureManualSapSchema(
  env
) {

  if (
    manualSapSchemaPromise
  ) {

    return manualSapSchemaPromise;

  }


  manualSapSchemaPromise =
    env.DB.batch([

      env.DB.prepare(
`CREATE TABLE IF NOT EXISTS manual_sap_control (
  tracking_code TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (
    status IN ('EN_SAP','RESUELTO')
  ),
  marked_sap_at TEXT NOT NULL,
  resolved_at TEXT,
  updated_at TEXT NOT NULL,
  customer_name TEXT,
  seller_name TEXT,
  order_date TEXT,
  order_total REAL NOT NULL DEFAULT 0
)`
      ),

      env.DB.prepare(
`CREATE INDEX IF NOT EXISTS idx_manual_sap_control_status
 ON manual_sap_control(status)`
      )

    ])
      .catch(
        error => {

          manualSapSchemaPromise =
            null;


          throw error;

        }
      );


  return manualSapSchemaPromise;

}


function requireTrackingCode(
  value
) {

  const tracking =
    normId(
      value
    );


  if (!tracking) {

    throw new Error(
      "Tracking inválido."
    );

  }


  return tracking;

}



async function manualSapMark(
  env,
  trackingValue
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureManualSapSchema(
    env
  );


  const tracking =
    requireTrackingCode(
      trackingValue
    );


  const order =
    await env.DB
      .prepare(
`SELECT
  tracking_code,
  customer_name,
  seller_name,
  order_date,
  order_total
FROM zas_orders
WHERE tracking_code = ?1`
      )
      .bind(
        tracking
      )
      .first();


  if (!order) {

    throw new Error(
      "La orden no existe en zas_orders."
    );

  }


  const now =
    new Date()
      .toISOString();


  await env.DB
    .prepare(
`INSERT INTO manual_sap_control (
  tracking_code,
  status,
  marked_sap_at,
  resolved_at,
  updated_at,
  customer_name,
  seller_name,
  order_date,
  order_total
)
VALUES (
  ?1,
  'EN_SAP',
  ?2,
  NULL,
  ?2,
  ?3,
  ?4,
  ?5,
  ?6
)
ON CONFLICT(tracking_code)
DO UPDATE SET
  status = 'EN_SAP',
  marked_sap_at = CASE
    WHEN manual_sap_control.marked_sap_at IS NULL
    THEN excluded.marked_sap_at
    ELSE manual_sap_control.marked_sap_at
  END,
  resolved_at = NULL,
  updated_at = excluded.updated_at,
  customer_name = excluded.customer_name,
  seller_name = excluded.seller_name,
  order_date = excluded.order_date,
  order_total = excluded.order_total`
      )
      .bind(
        tracking,
        now,
        order.customer_name,
        order.seller_name,
        order.order_date,
        numberValue(
          order.order_total
        )
      )
      .run();


  return {
    ok: true,

    trackingCode:
      tracking,

    status:
      "EN_SAP",

    message:
      "Orden movida a Control SAP."
  };

}



async function manualSapResolve(
  env,
  trackingValue
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureManualSapSchema(
    env
  );


  const tracking =
    requireTrackingCode(
      trackingValue
    );


  const existing =
    await env.DB
      .prepare(
`SELECT
  tracking_code
FROM manual_sap_control
WHERE tracking_code = ?1`
      )
      .bind(
        tracking
      )
      .first();


  if (!existing) {

    throw new Error(
      "La orden no está en Control SAP."
    );

  }


  const now =
    new Date()
      .toISOString();


  await env.DB
    .prepare(
`UPDATE manual_sap_control
SET
  status = 'RESUELTO',
  resolved_at = ?2,
  updated_at = ?2
WHERE tracking_code = ?1`
      )
      .bind(
        tracking,
        now
      )
      .run();


  return {
    ok: true,

    trackingCode:
      tracking,

    status:
      "RESUELTO",

    virtualLoaded:
      true,

    message:
      "Orden resuelta. Se considera cargada aunque no exista físicamente en BO/VT."
  };

}



async function manualSapList(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  await ensureManualSapSchema(
    env
  );


  const result =
    await env.DB
      .prepare(
`SELECT
  tracking_code,
  status,
  marked_sap_at,
  resolved_at,
  updated_at,
  customer_name,
  seller_name,
  order_date,
  order_total
FROM manual_sap_control
ORDER BY
  CASE status
    WHEN 'EN_SAP' THEN 1
    WHEN 'RESUELTO' THEN 2
    ELSE 9
  END,
  updated_at DESC`
      )
      .all();


  const rows =
    result?.results ||
    [];


  return {
    ok: true,

    source:
      "D1",

    active:
      rows.filter(
        row =>
          row.status ===
          "EN_SAP"
      ),

    resolved:
      rows.filter(
        row =>
          row.status ===
          "RESUELTO"
      ),

    counts: {
      active:
        rows.filter(
          row =>
            row.status ===
            "EN_SAP"
        )
          .length,

      resolved:
        rows.filter(
          row =>
            row.status ===
            "RESUELTO"
        )
          .length
    }
  };

}




function gtDateOnly(
  date =
    new Date()
) {

  const parts =
    new Intl.DateTimeFormat(
      "en-CA",
      {
        timeZone:
          "America/Guatemala",
        year:
          "numeric",
        month:
          "2-digit",
        day:
          "2-digit"
      }
    )
      .formatToParts(
        date
      );


  const map =
    Object.fromEntries(
      parts.map(
        part => [
          part.type,
          part.value
        ]
      )
    );


  return (
    map.year +
    "-" +
    map.month +
    "-" +
    map.day
  );

}


function gtDaysAgo(
  days
) {

  return gtDateOnly(
    new Date(
      Date.now() -
      days *
      86400000
    )
  );

}


async function getZasOrdersRecent(
  env,
  days = 7
) {

  const cutoff =
    gtDaysAgo(
      days
    );


  const cookie =
    await loginZasConnect(
      env
    );


  let page = 1;
  let totalPages = 1;
  let pageSize = 25;

  const rows = [];


  while (
    page <=
    totalPages
  ) {

    const result =
      await fetchZasPage(
        cookie,
        page,
        pageSize
      );


    if (
      page ===
      1
    ) {

      pageSize =
        Number(
          result.pagination?.pageSize ||
          25
        ) ||
        25;


      const total =
        Number(
          result.pagination?.total ||
          0
        ) ||
        0;


      totalPages =
        total > 0
          ? Math.max(
              1,
              Math.ceil(
                total /
                pageSize
              )
            )
          : 1;

    }


    const pageRows =
      result.rows ||
      [];


    const dates =
      pageRows
        .map(
          row =>
            dateOnly(
              row.orderDate
            )
        )
        .filter(Boolean);


    rows.push(
      ...pageRows.filter(
        row => {

          const value =
            dateOnly(
              row.orderDate
            );


          return (
            value &&
            value >=
            cutoff
          );

        }
      )
    );


    const newestOnPage =
      dates.length
        ? dates
            .slice()
            .sort()
            .at(-1)
        : null;


    // ZasConnect entrega las páginas de más reciente a más antigua.
    // Cuando toda la página ya quedó antes del corte no seguimos leyendo historia.
    if (
      newestOnPage &&
      newestOnPage <
      cutoff
    ) {
      break;
    }


    page++;

  }


  const uniqueRows = [];
  const seen = new Set();


  for (
    const row
    of rows
  ) {

    const key =
      JSON.stringify(
        row
      );


    if (
      seen.has(
        key
      )
    ) {
      continue;
    }


    seen.add(
      key
    );


    uniqueRows.push(
      row
    );

  }


  return {
    cutoff,
    rows:
      uniqueRows
  };

}


async function d1DeleteTrackingChunks(
  db,
  table,
  trackings
) {

  if (
    !trackings.length
  ) {
    return 0;
  }


  let queries = 0;


  for (
    let i = 0;
    i < trackings.length;
    i += 80
  ) {

    const chunk =
      trackings.slice(
        i,
        i + 80
      );


    const placeholders =
      chunk
        .map(
          (
            _,
            index
          ) =>
            `?${index + 1}`
        )
        .join(
          ","
        );


    await db
      .prepare(
        `DELETE FROM ${table} WHERE tracking_code IN (${placeholders})`
      )
      .bind(
        ...chunk
      )
      .run();


    queries++;

  }


  return queries;

}


function d1HeaderToApiRow(
  row
) {

  return {
    trackingCode:
      row.tracking_code,

    sellerName:
      row.seller_name,

    orderDate:
      row.order_date,

    orderTotal:
      row.order_total
  };

}


function d1LoadedRefToDetailRow(
  row
) {

  let group =
    row.group_name ||
    "K3";


  if (
    Number(
      row.is_pdv
    )
  ) {

    if (
      row.channel ===
      "PETAPA"
    ) {

      group =
        "K1 PISO PETAPA";

    } else if (
      row.channel ===
      "XENACOJ"
    ) {

      group =
        "K1 PISO XENACOJ";

    }

  }


  return {
    source:
      Number(
        row.has_vt
      )
        ? "VT"
        : "BO",

    uId:
      row.tracking_code,

    type:
      Number(
        row.is_pdv
      )
        ? "PDV"
        : "DIGITAL",

    date:
      row.report_date,

    route:
      row.route,

    reportAdvisor:
      row.route,

    group,

    amount:
      numberValue(
        row.chosen_amount
      )
  };

}


async function rebuildReportOrdersFromD1(
  env,
  now
) {

  const [
    orderResult,
    refResult,
    existingResult
  ] =
    await env.DB.batch([

      env.DB.prepare(
`SELECT
  tracking_code,
  seller_name,
  order_date,
  order_total
FROM zas_orders`
      ),

      env.DB.prepare(
`SELECT
  tracking_code,
  is_pdv,
  has_bo,
  has_vt,
  report_date,
  route,
  channel,
  group_name,
  chosen_amount
FROM loaded_refs`
      ),

      env.DB.prepare(
`SELECT
  tracking_code,
  report_date,
  advisor,
  seller_original,
  order_type,
  group_name,
  level,
  channel,
  amount,
  source,
  source_hash
FROM report_orders`
      )

    ]);


  const apiRows =
    (
      orderResult?.results ||
      []
    )
      .map(
        d1HeaderToApiRow
      );


  const detail =
    (
      refResult?.results ||
      []
    )
      .map(
        d1LoadedRefToDetailRow
      );


  const desired =
    buildReportRowsForD1(
      apiRows,
      detail
    );


  const existing =
    new Map(
      (
        existingResult?.results ||
        []
      )
        .map(
          row => [
            row.tracking_code,
            row
          ]
        )
    );


  const desiredMap =
    new Map(
      desired.map(
        row => [
          row.tracking_code,
          row
        ]
      )
    );


  const changed =
    desired.filter(
      row => {

        const current =
          existing.get(
            row.tracking_code
          );


        if (!current) {
          return true;
        }


        return (
          current.source_hash !==
            row.source_hash ||

          String(
            current.report_date ||
            ""
          ) !==
            String(
              row.report_date ||
              ""
            ) ||

          normText(
            current.advisor
          ) !==
            normText(
              row.advisor
            ) ||

          normText(
            current.seller_original
          ) !==
            normText(
              row.seller_original
            ) ||

          String(
            current.order_type ||
            ""
          ) !==
            String(
              row.order_type ||
              ""
            ) ||

          String(
            current.group_name ||
            ""
          ) !==
            String(
              row.group_name ||
              ""
            ) ||

          String(
            current.level ||
            ""
          ) !==
            String(
              row.level ||
              ""
            ) ||

          String(
            current.channel ||
            ""
          ) !==
            String(
              row.channel ||
              ""
            ) ||

          Math.abs(
            numberValue(
              current.amount
            ) -
            numberValue(
              row.amount
            )
          ) >
            0.01 ||

          String(
            current.source ||
            ""
          ) !==
            String(
              row.source ||
              ""
            )
        );

      }
    );


  const deleted =
    [
      ...existing.keys()
    ]
      .filter(
        tracking =>
          !desiredMap.has(
            tracking
          )
      );


  let deleteQueries = 0;


  if (
    deleted.length
  ) {

    deleteQueries =
      await d1DeleteTrackingChunks(
        env.DB,
        "report_orders",
        deleted
      );

  }


  const write =
    await d1BulkInsertJson(
      env.DB,
      "report_orders",
      [
        "tracking_code",
        "report_date",
        "advisor",
        "seller_original",
        "order_type",
        "group_name",
        "level",
        "channel",
        "amount",
        "source",
        "source_hash",
        "synced_at"
      ],
      changed.map(
        row => ({
          ...row,
          synced_at:
            now
        })
      ),
      {
        replace:
          true
      }
    );


  const dates =
    desired
      .map(
        row =>
          row.report_date
      )
      .filter(Boolean)
      .sort();


  return {
    seen:
      desired.length,

    changed:
      changed.length,

    deleted:
      deleted.length,

    writeQueries:
      write.queries,

    deleteQueries,

    rangeFrom:
      dates[0] ||
      null,

    rangeTo:
      dates.at(-1) ||
      null
  };

}



async function detectReportMaterializationDrift(
  env
) {

  const result =
    await env.DB
      .prepare(
`WITH drift AS (

  -- Si report_orders dice ZASCONNECT, debe existir la orden API.
  SELECT
    r.tracking_code

  FROM report_orders AS r

  LEFT JOIN zas_orders AS z
    ON z.tracking_code =
       r.tracking_code

  WHERE r.source = 'ZASCONNECT'
    AND z.tracking_code IS NULL


  UNION


  -- Si el tracking existe en ZasConnect, ZasConnect es dueño
  -- de fuente, fecha y monto del reporte.
  SELECT
    r.tracking_code

  FROM report_orders AS r

  JOIN zas_orders AS z
    ON z.tracking_code =
       r.tracking_code

  WHERE r.source <> 'ZASCONNECT'

     OR r.report_date <>
        z.order_date

     OR ABS(
          COALESCE(
            r.amount,
            0
          ) -
          COALESCE(
            z.order_total,
            0
          )
        ) > 0.01


  UNION


  -- Toda orden API debe tener fila materializada.
  SELECT
    z.tracking_code

  FROM zas_orders AS z

  LEFT JOIN report_orders AS r
    ON r.tracking_code =
       z.tracking_code

  WHERE r.tracking_code IS NULL


  UNION


  -- BO/VT puede originar una venta independiente únicamente cuando
  -- U_IDFRONT viene vacío y usamos el tracking sintético PDV-NOID.
  SELECT
    lr.tracking_code

  FROM loaded_refs AS lr

  LEFT JOIN zas_orders AS z
    ON z.tracking_code =
       lr.tracking_code

  LEFT JOIN report_orders AS r
    ON r.tracking_code =
       lr.tracking_code

  WHERE lr.is_pdv = 1
    AND z.tracking_code IS NULL
    AND lr.tracking_code LIKE 'PDV-NOID|%'
    AND (
      r.tracking_code IS NULL
      OR r.source <> 'PDV BO/VT'
      OR r.report_date <> lr.report_date
      OR UPPER(
           TRIM(
             COALESCE(
               r.advisor,
               ''
             )
           )
         ) <>
         UPPER(
           TRIM(
             COALESCE(
               lr.route,
               ''
             )
           )
         )
      OR ABS(
           COALESCE(
             r.amount,
             0
           ) -
           COALESCE(
             lr.chosen_amount,
             0
           )
         ) > 0.01
    )

)

SELECT
  COUNT(*) AS n
FROM drift`
      )
      .first();


  return Number(
    result?.n ||
    0
  );

}




// ============================================================
// IMPORTADOR EXCEL -> D1 COMO FUENTE API MANUAL
// ============================================================

let manualApiImportSchemaPromise =
  null;


async function ensureManualApiImportSchema(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  if (
    manualApiImportSchemaPromise
  ) {

    return manualApiImportSchemaPromise;

  }


  manualApiImportSchemaPromise =
    (async () => {

      await env.DB.batch([

        env.DB.prepare(
`CREATE TABLE IF NOT EXISTS manual_zas_api_registry (
  tracking_code TEXT PRIMARY KEY,
  import_id TEXT NOT NULL,
  file_name TEXT,
  row_count INTEGER NOT NULL DEFAULT 0,
  order_total REAL NOT NULL DEFAULT 0,
  imported_at TEXT NOT NULL,
  api_seen_at TEXT
)`
        ),

        env.DB.prepare(
`CREATE TABLE IF NOT EXISTS manual_zas_api_imports (
  import_id TEXT PRIMARY KEY,
  file_name TEXT,
  imported_at TEXT NOT NULL,
  source_rows INTEGER NOT NULL DEFAULT 0,
  unique_orders INTEGER NOT NULL DEFAULT 0,
  imported_orders INTEGER NOT NULL DEFAULT 0,
  updated_manual_orders INTEGER NOT NULL DEFAULT 0,
  skipped_existing_api INTEGER NOT NULL DEFAULT 0,
  imported_lines INTEGER NOT NULL DEFAULT 0,
  total_amount REAL NOT NULL DEFAULT 0,
  report_rows_changed INTEGER NOT NULL DEFAULT 0
)`
        ),

        env.DB.prepare(
`CREATE INDEX IF NOT EXISTS idx_manual_zas_api_registry_import
ON manual_zas_api_registry(import_id)`
        )

      ]);

    })()
      .catch(
        error => {

          manualApiImportSchemaPromise =
            null;

          throw error;

        }
      );


  return manualApiImportSchemaPromise;

}


async function manualApiProtectedTrackings(
  env
) {

  await ensureManualApiImportSchema(
    env
  );


  const result =
    await env.DB
      .prepare(
`SELECT tracking_code
FROM manual_zas_api_registry`
      )
      .all();


  return new Set(
    (
      result?.results ||
      []
    )
      .map(
        row =>
          normId(
            row.tracking_code
          )
      )
      .filter(Boolean)
  );

}


async function markManualTrackingsSeenInApi(
  env,
  trackings,
  seenAt =
    new Date()
      .toISOString()
) {

  const normalized =
    [
      ...new Set(
        (trackings || [])
          .map(normId)
          .filter(Boolean)
      )
    ];


  if (!normalized.length) {
    return 0;
  }


  await ensureManualApiImportSchema(
    env
  );


  let queries = 0;


  for (
    let i = 0;
    i < normalized.length;
    i += 80
  ) {

    const chunk =
      normalized.slice(
        i,
        i + 80
      );


    const placeholders =
      chunk
        .map(
          (
            _,
            index
          ) =>
            `?${index + 2}`
        )
        .join(",");


    await env.DB
      .prepare(
`UPDATE manual_zas_api_registry
SET api_seen_at = ?1
WHERE tracking_code IN (${placeholders})`
      )
      .bind(
        seenAt,
        ...chunk
      )
      .run();


    queries++;

  }


  return queries;

}


function cleanManualApiImportRow(
  row
) {

  const allowed = [
    "customerTrackingCode",
    "taxId",
    "customerName",
    "primaryPhone",
    "secondaryPhone",
    "sellerName",
    "shippingMunicipality",
    "shippingReference",
    "custom_rcf",
    "salesChannel",
    "shippingDepartment",
    "orderDate",
    "sku",
    "description",
    "quantity",
    "brand",
    "locationCode",
    "ringCode",
    "comments",
    "trackingCode",
    "warehouse",
    "orderTotal",
    "unitPrice",
    "lineNumber",
    "shippingAvenue",
    "shippingHouseNumber",
    "shippingZone",
    "lineTotal",
    "custom_bodega",
    "custom_canal",
    "custom_payment_method",
    "custom_marca"
  ];


  const out = {};


  for (
    const field
    of allowed
  ) {

    out[field] =
      row?.[field] ??
      null;

  }


  out.trackingCode =
    normId(
      out.trackingCode
    );


  out.orderDate =
    dateOnly(
      out.orderDate
    );


  out.orderTotal =
    round2(
      numberValue(
        out.orderTotal
      )
    );


  out.quantity =
    numberValue(
      out.quantity
    );


  out.unitPrice =
    round2(
      numberValue(
        out.unitPrice
      )
    );


  out.lineTotal =
    round2(
      numberValue(
        out.lineTotal
      )
    );


  return out;

}


function validateManualApiImportRows(
  rawRows
) {

  if (
    !Array.isArray(
      rawRows
    ) ||
    !rawRows.length
  ) {

    throw new Error(
      "El Excel no contiene filas para importar."
    );

  }


  if (
    rawRows.length >
    10000
  ) {

    throw new Error(
      "El importador admite un máximo de 10,000 líneas por archivo."
    );

  }


  const rows =
    rawRows.map(
      cleanManualApiImportRow
    );


  const withoutTracking =
    rows.filter(
      row =>
        !row.trackingCode
    ).length;


  if (withoutTracking) {

    throw new Error(
      "Hay " +
      withoutTracking +
      " fila(s) sin Tracking pedido. Corrige el Excel antes de importar."
    );

  }


  const groups =
    new Map();


  for (
    const row
    of rows
  ) {

    if (
      !groups.has(
        row.trackingCode
      )
    ) {

      groups.set(
        row.trackingCode,
        []
      );

    }


    groups
      .get(
        row.trackingCode
      )
      .push(
        row
      );

  }


  const errors = [];


  for (
    const [
      tracking,
      group
    ]
    of groups.entries()
  ) {

    const first =
      group[0];


    const date =
      dateOnly(
        first.orderDate
      );


    const total =
      round2(
        numberValue(
          first.orderTotal
        )
      );


    const seller =
      normText(
        first.sellerName
      );


    const customer =
      normText(
        first.customerName
      );


    for (
      const row
      of group
    ) {

      if (
        dateOnly(
          row.orderDate
        ) !==
        date ||
        Math.abs(
          numberValue(
            row.orderTotal
          ) -
          total
        ) >
        0.01 ||
        normText(
          row.sellerName
        ) !==
        seller ||
        normText(
          row.customerName
        ) !==
        customer
      ) {

        errors.push(
          "Tracking " +
          tracking +
          ": los datos de cabecera cambian entre sus líneas."
        );

        break;

      }

    }

  }


  if (
    errors.length
  ) {

    throw new Error(
      errors
        .slice(
          0,
          8
        )
        .join(" ")
    );

  }


  return {
    rows,
    groups
  };

}


async function selectTrackingRowsInChunks(
  env,
  sqlPrefix,
  trackings
) {

  const rows = [];


  for (
    let i = 0;
    i < trackings.length;
    i += 80
  ) {

    const chunk =
      trackings.slice(
        i,
        i + 80
      );


    if (!chunk.length) {
      continue;
    }


    const placeholders =
      chunk
        .map(
          (
            _,
            index
          ) =>
            `?${index + 1}`
        )
        .join(",");


    const result =
      await env.DB
        .prepare(
          sqlPrefix +
          ` (${placeholders})`
        )
        .bind(
          ...chunk
        )
        .all();


    rows.push(
      ...(
        result?.results ||
        []
      )
    );

  }


  return rows;

}


async function manualApiImportRows(
  env,
  body
) {

  await ensureManualApiImportSchema(
    env
  );


  const validated =
    validateManualApiImportRows(
      body?.rows
    );


  const grouped =
    groupZasForD1(
      validated.rows
    );


  const trackings =
    grouped.orders.map(
      row =>
        row.tracking_code
    );


  const existingOrders =
    await selectTrackingRowsInChunks(
      env,
      "SELECT tracking_code FROM zas_orders WHERE tracking_code IN",
      trackings
    );


  const registryRows =
    await selectTrackingRowsInChunks(
      env,
      "SELECT tracking_code, api_seen_at FROM manual_zas_api_registry WHERE tracking_code IN",
      trackings
    );


  const existingSet =
    new Set(
      existingOrders.map(
        row =>
          normId(
            row.tracking_code
          )
      )
    );


  const registryMap =
    new Map(
      registryRows.map(
        row => [
          normId(
            row.tracking_code
          ),
          row
        ]
      )
    );


  const skippedExistingApi =
    new Set();

  const updatedManual =
    new Set();

  const importTrackingSet =
    new Set();


  for (
    const tracking
    of trackings
  ) {

    const registry =
      registryMap.get(
        tracking
      );


    if (
      registry?.api_seen_at
    ) {

      skippedExistingApi.add(
        tracking
      );

      continue;

    }


    if (
      existingSet.has(
        tracking
      ) &&
      !registry
    ) {

      skippedExistingApi.add(
        tracking
      );

      continue;

    }


    importTrackingSet.add(
      tracking
    );


    if (registry) {

      updatedManual.add(
        tracking
      );

    }

  }


  const ordersToWrite =
    grouped.orders.filter(
      row =>
        importTrackingSet.has(
          row.tracking_code
        )
    );


  const linesToWrite =
    grouped.lines.filter(
      row =>
        importTrackingSet.has(
          row.tracking_code
        )
    );


  const now =
    new Date()
      .toISOString();


  const importId =
    crypto.randomUUID();


  const fileName =
    String(
      body?.fileName ||
      "importacion.xlsx"
    )
      .slice(
        0,
        240
      );


  if (
    importTrackingSet.size
  ) {

    // Una reimportación manual reemplaza exactamente las líneas manuales
    // anteriores de esos tracking para no dejar SKU obsoletos.
    await d1DeleteTrackingChunks(
      env.DB,
      "zas_order_lines",
      [
        ...importTrackingSet
      ]
    );


    await d1BulkInsertJson(
      env.DB,
      "zas_orders",
      [
        "tracking_code",
        "customer_tracking_code",
        "tax_id",
        "customer_name",
        "primary_phone",
        "secondary_phone",
        "seller_name",
        "shipping_municipality",
        "shipping_reference",
        "custom_rcf",
        "sales_channel",
        "shipping_department",
        "order_date",
        "comments",
        "warehouse",
        "order_total",
        "shipping_avenue",
        "shipping_house_number",
        "shipping_zone",
        "custom_bodega",
        "custom_canal",
        "custom_payment_method",
        "custom_marca",
        "source_hash",
        "source_updated_at",
        "synced_at"
      ],
      ordersToWrite.map(
        row => ({
          ...row,
          synced_at:
            now
        })
      ),
      {
        replace:
          true
      }
    );


    await d1BulkInsertJson(
      env.DB,
      "zas_order_lines",
      [
        "tracking_code",
        "line_key",
        "line_number",
        "sku",
        "description",
        "quantity",
        "brand",
        "location_code",
        "ring_code",
        "unit_price",
        "line_total",
        "source_hash",
        "synced_at"
      ],
      linesToWrite.map(
        row => ({
          ...row,
          synced_at:
            now
        })
      ),
      {
        replace:
          true
      }
    );


    const lineCounts =
      new Map();


    for (
      const line
      of linesToWrite
    ) {

      lineCounts.set(
        line.tracking_code,
        (
          lineCounts.get(
            line.tracking_code
          ) ||
          0
        ) +
        1
      );

    }


    // --------------------------------------------------------
    // VERIFICACIÓN POST-ESCRITURA
    //
    // Si el archivo dice que un tracking tiene N líneas,
    // D1 debe terminar exactamente con N líneas.
    // --------------------------------------------------------

    const storedLines =
      await selectTrackingRowsInChunks(
        env,
        "SELECT tracking_code, line_key, sku FROM zas_order_lines WHERE tracking_code IN",
        [
          ...importTrackingSet
        ]
      );


    const expectedLineCounts =
      new Map();


    for (
      const line
      of linesToWrite
    ) {

      expectedLineCounts.set(
        line.tracking_code,
        (
          expectedLineCounts.get(
            line.tracking_code
          ) ||
          0
        ) +
        1
      );

    }


    const storedLineCounts =
      new Map();


    for (
      const line
      of storedLines
    ) {

      const tracking =
        normId(
          line.tracking_code
        );


      storedLineCounts.set(
        tracking,
        (
          storedLineCounts.get(
            tracking
          ) ||
          0
        ) +
        1
      );

    }


    const lineCountErrors =
      [];


    for (
      const [
        tracking,
        expected
      ]
      of expectedLineCounts
    ) {

      const stored =
        storedLineCounts.get(
          tracking
        ) ||
        0;


      if (
        stored !==
        expected
      ) {

        lineCountErrors.push(
          tracking +
          ": esperado " +
          expected +
          ", guardado " +
          stored
        );

      }

    }


    if (
      lineCountErrors.length
    ) {

      throw new Error(
        "La verificación de líneas D1 falló. " +
        lineCountErrors
          .slice(
            0,
            10
          )
          .join(
            " | "
          )
      );

    }


    await d1BulkInsertJson(
      env.DB,
      "manual_zas_api_registry",
      [
        "tracking_code",
        "import_id",
        "file_name",
        "row_count",
        "order_total",
        "imported_at",
        "api_seen_at"
      ],
      ordersToWrite.map(
        row => ({
          tracking_code:
            row.tracking_code,
          import_id:
            importId,
          file_name:
            fileName,
          row_count:
            lineCounts.get(
              row.tracking_code
            ) ||
            0,
          order_total:
            row.order_total,
          imported_at:
            now,
          api_seen_at:
            null
        })
      ),
      {
        replace:
          true
      }
    );

  }


  const importedTotal =
    round2(
      ordersToWrite.reduce(
        (
          sum,
          row
        ) =>
          sum +
          numberValue(
            row.order_total
          ),
        0
      )
    );


  const reportStats =
    await rebuildReportOrdersFromD1(
      env,
      now
    );


  await env.DB
    .prepare(
`INSERT INTO manual_zas_api_imports (
  import_id,
  file_name,
  imported_at,
  source_rows,
  unique_orders,
  imported_orders,
  updated_manual_orders,
  skipped_existing_api,
  imported_lines,
  total_amount,
  report_rows_changed
)
VALUES (
  ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11
)`
    )
    .bind(
      importId,
      fileName,
      now,
      validated.rows.length,
      grouped.orders.length,
      ordersToWrite.length,
      updatedManual.size,
      skippedExistingApi.size,
      linesToWrite.length,
      importedTotal,
      Number(
        reportStats?.changed ||
        0
      )
    )
    .run();


  return {
    ok:
      true,
    importId,
    fileName,
    sourceRows:
      validated.rows.length,
    uniqueOrders:
      grouped.orders.length,
    importedOrders:
      ordersToWrite.length,
    newManualOrders:
      ordersToWrite.length -
      updatedManual.size,
    updatedManualOrders:
      updatedManual.size,
    skippedExistingApi:
      skippedExistingApi.size,
    importedLines:
      linesToWrite.length,
    verifiedStoredLines:
      linesToWrite.length,
    lineStorageVerified:
      true,
    totalAmount:
      importedTotal,
    reports:
      reportStats,
    behavior: {
      orderKey:
        "Tracking pedido",
      orderTotalCountedOnce:
        true,
      productLinesPreserved:
        true,
      existingApiHasPriority:
        true,
      protectedFromApiCleanup:
        true
    }
  };

}


async function manualApiImportList(
  env
) {

  await ensureManualApiImportSchema(
    env
  );


  const [
    imports,
    registry
  ] =
    await env.DB.batch([

      env.DB.prepare(
`SELECT
  import_id,
  file_name,
  imported_at,
  source_rows,
  unique_orders,
  imported_orders,
  updated_manual_orders,
  skipped_existing_api,
  imported_lines,
  total_amount,
  report_rows_changed
FROM manual_zas_api_imports
ORDER BY imported_at DESC
LIMIT 20`
      ),

      env.DB.prepare(
`SELECT
  COUNT(*) AS orders,
  COALESCE(SUM(row_count), 0) AS lines,
  COALESCE(SUM(order_total), 0) AS total,
  SUM(CASE WHEN api_seen_at IS NOT NULL THEN 1 ELSE 0 END) AS api_seen
FROM manual_zas_api_registry`
      )

    ]);


  const current =
    registry?.results?.[0] ||
    {};


  return {
    ok:
      true,
    current: {
      protectedOrders:
        Number(
          current.orders ||
          0
        ),
      productLines:
        Number(
          current.lines ||
          0
        ),
      totalAmount:
        round2(
          numberValue(
            current.total
          )
        ),
      nowAlsoSeenInApi:
        Number(
          current.api_seen ||
          0
        )
    },
    imports:
      imports?.results ||
      []
  };

}


async function reconcileRecentLoadedRefsAgainstLiveApi(
  env,
  cutoff
) {

  const candidateResult =
    await env.DB
      .prepare(
`SELECT
  z.tracking_code,
  z.order_date,
  z.seller_name,
  z.order_total,
  lr.report_date,
  lr.route,
  lr.is_pdv,
  lr.channel,
  lr.chosen_amount

FROM zas_orders AS z

JOIN loaded_refs AS lr
  ON lr.tracking_code =
     z.tracking_code

WHERE lr.report_date >= ?1

ORDER BY
  lr.report_date DESC,
  z.tracking_code`
      )
      .bind(
        cutoff
      )
      .all();


  const candidates =
    candidateResult?.results ||
    [];


  if (!candidates.length) {

    return {
      checked:
        0,
      fullApiFetched:
        false,
      deleted:
        [],
      kept:
        []
    };

  }


  // La respuesta reciente puede no incluir una orden antigua que todavía
  // existe realmente en ZasConnect. Por eso NO borramos basándonos solo
  // en la ventana de 7 días. Verificamos contra el API completo actual.
  const liveApiRows =
    await getZasOrders(
      env
    );


  const liveTrackings =
    new Set(
      (
        liveApiRows ||
        []
      )
        .map(
          row =>
            normId(
              row.trackingCode
            )
        )
        .filter(Boolean)
    );


  const manualProtected =
    await manualApiProtectedTrackings(
      env
    );


  for (
    const tracking
    of manualProtected
  ) {

    liveTrackings.add(
      tracking
    );

  }


  await markManualTrackingsSeenInApi(
    env,
    (liveApiRows || [])
      .map(
        row =>
          row.trackingCode
      )
  );


  const stale =
    candidates.filter(
      row =>
        !liveTrackings.has(
          normId(
            row.tracking_code
          )
        )
    );


  const kept =
    candidates.filter(
      row =>
        liveTrackings.has(
          normId(
            row.tracking_code
          )
        )
    );


  const staleTrackings =
    stale.map(
      row =>
        row.tracking_code
    );


  let deleteQueries =
    0;


  if (
    staleTrackings.length
  ) {

    deleteQueries +=
      await d1DeleteTrackingChunks(
        env.DB,
        "zas_order_lines",
        staleTrackings
      );


    deleteQueries +=
      await d1DeleteTrackingChunks(
        env.DB,
        "zas_orders",
        staleTrackings
      );

  }


  return {
    checked:
      candidates.length,

    fullApiFetched:
      true,

    liveApiRows:
      liveApiRows?.length ||
      0,

    deleted:
      staleTrackings,

    kept:
      kept.map(
        row =>
          row.tracking_code
      ),

    deleteQueries
  };

}



let reportFreshRepairPromise =
  null;




async function countTrackedApiAttributionMismatches(
  env
) {

  const row =
    await env.DB
      .prepare(
`SELECT
  COUNT(*) AS n

FROM report_orders AS r

JOIN zas_orders AS z
  ON z.tracking_code =
     r.tracking_code

WHERE r.source <> 'ZASCONNECT'

   OR r.report_date <>
      z.order_date

   OR ABS(
        COALESCE(
          r.amount,
          0
        ) -
        COALESCE(
          z.order_total,
          0
        )
      ) > 0.01`
      )
      .first();


  return Number(
    row?.n ||
    0
  );

}



async function countPdvDateAttributionMismatches(
  env
) {

  // Compatibilidad con el diagnóstico anterior.
  // Ahora el criterio correcto es propiedad del tracking ZasConnect.
  return await countTrackedApiAttributionMismatches(
    env
  );

}



async function countCrossSourceOverlaps(
  env
) {

  const row =
    await env.DB
      .prepare(
`SELECT
  COUNT(*) AS n

FROM zas_orders AS z

JOIN loaded_refs AS lr
  ON lr.tracking_code =
     z.tracking_code

WHERE lr.is_pdv = 1`
      )
      .first();


  return Number(
    row?.n ||
    0
  );

}



async function countCrossSourceReportCollisions(
  env
) {

  // "Colisión" ahora significa conflicto real de atribución,
  // no simplemente que el tracking exista en ambas fuentes.
  return await countTrackedApiAttributionMismatches(
    env
  );

}



async function ensureReportOrdersFresh(
  env
) {

  if (
    reportFreshRepairPromise
  ) {

    return reportFreshRepairPromise;

  }


  reportFreshRepairPromise =
    (
      async () => {

        const before =
          await detectReportMaterializationDrift(
            env
          );


        if (
          before <=
          0
        ) {

          return {
            repaired:
              false,
            before:
              0,
            after:
              0
          };

        }


        const now =
          new Date()
            .toISOString();


        const stats =
          await rebuildReportOrdersFromD1(
            env,
            now
          );


        const after =
          await detectReportMaterializationDrift(
            env
          );


        await env.DB
          .prepare(
`INSERT INTO sync_state (
  source,
  last_started_at,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json,
  last_error
)
VALUES (
  'REPORT_AUTO_REPAIR',
  ?1,
  ?1,
  ?2,
  ?3,
  ?4,
  NULL
)
ON CONFLICT(source)
DO UPDATE SET
  last_started_at = excluded.last_started_at,
  last_success_at = excluded.last_success_at,
  records_seen = excluded.records_seen,
  records_changed = excluded.records_changed,
  metadata_json = excluded.metadata_json,
  last_error = NULL`
          )
          .bind(
            now,
            stats.seen,
            stats.changed +
            stats.deleted,
            JSON.stringify({
              before,
              after,
              changed:
                stats.changed,
              deleted:
                stats.deleted
            })
          )
          .run();


        return {
          repaired:
            true,
          before,
          after,
          stats
        };

      }
    )()
      .finally(
        () => {

          reportFreshRepairPromise =
            null;

        }
      );


  return reportFreshRepairPromise;

}




// ============================================================
// POSTGRESQL - CAPA DE COMPATIBILIDAD CON EL MOTOR D1
// Fase 5.26.3
//
// Permite reutilizar la lógica de sincronización ya validada sin
// reescribir de golpe cientos de consultas. Solo se usa en la ruta
// manual /api/postgres-sync-test durante esta fase.
// ============================================================

const PG_COMPAT_PRIMARY_KEYS = {
  loaded_refs: [
    "tracking_code"
  ],

  manual_sap_control: [
    "tracking_code"
  ],

  manual_zas_api_imports: [
    "import_id"
  ],

  manual_zas_api_registry: [
    "tracking_code"
  ],

  pending_name_normalization: [
    "id"
  ],

  report_group_labels: [
    "level"
  ],

  report_orders: [
    "tracking_code"
  ],

  report_type_config: [
    "config_key"
  ],

  report_type_value_rules: [
    "field_name",
    "field_value_norm"
  ],

  seller_monthly_targets: [
    "seller_key",
    "month"
  ],

  seller_report_match: [
    "match_key"
  ],

  seller_report_rule_rows: [
    "normalization_id"
  ],

  sync_state: [
    "source"
  ],

  zas_order_lines: [
    "tracking_code",
    "line_key"
  ],

  zas_orders: [
    "tracking_code"
  ]
};



function pgCompatQuoteIdent(
  value
) {

  const text =
    String(
      value ||
      ""
    );


  if (
    !/^[A-Za-z_][A-Za-z0-9_]*$/
      .test(
        text
      )
  ) {

    throw new Error(
      "Identificador SQL no permitido: " +
      text
    );

  }


  return (
    '"' +
    text +
    '"'
  );

}



function pgCompatTranslateSql(
  inputSql
) {

  let sql =
    String(
      inputSql ||
      ""
    )
      .trim();


  if (!sql) {

    return {
      skip:
        true,
      sql:
        ""
    };

  }


  // Las 15 tablas e índices ya existen en Supabase.
  // No recrearlos con sintaxis SQLite.
  if (
    /^CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)\b/i
      .test(
        sql
      )
  ) {

    return {
      skip:
        true,
      sql
    };

  }


  let insertOrIgnore =
    false;


  if (
    /\bINSERT\s+OR\s+IGNORE\s+INTO\b/i
      .test(
        sql
      )
  ) {

    insertOrIgnore =
      true;

    sql =
      sql.replace(
        /\bINSERT\s+OR\s+IGNORE\s+INTO\b/i,
        "INSERT INTO"
      );

  }


  if (
    /\bINSERT\s+OR\s+REPLACE\s+INTO\b/i
      .test(
        sql
      )
  ) {

    throw new Error(
      "INSERT OR REPLACE debe pasar por bulkInsertJson en PostgreSQL."
    );

  }


  sql =
    sql
      .replace(
        /\?([1-9][0-9]*)/g,
        (_match, index) =>
          "$" +
          index
      )
      .replace(
        /\s+COLLATE\s+NOCASE\b/gi,
        ""
      )
      .replace(
        /datetime\s*\(\s*'now'\s*\)/gi,
        "to_char(clock_timestamp() at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS')"
      );


  if (
    insertOrIgnore &&
    !/\bON\s+CONFLICT\b/i
      .test(
        sql
      )
  ) {

    sql =
      sql.replace(
        /;\s*$/,
        ""
      ) +
      "\nON CONFLICT DO NOTHING";

  }


  return {
    skip:
      false,
    sql
  };

}



function pgCompatResult(
  result
) {

  const rows =
    result?.rows ||
    [];


  return {
    success:
      true,

    results:
      rows,

    meta: {
      changes:
        Number(
          result?.rowCount ||
          0
        ),

      duration:
        0
    }
  };

}



function makePgD1Compat(
  client
) {

  const compat = {

    __engine:
      "POSTGRESQL_D1_COMPAT",


    prepare(
      sql
    ) {

      return {
        sql:
          String(
            sql ||
            ""
          ),

        params:
          [],


        bind(
          ...params
        ) {

          this.params =
            params;

          return this;

        },


        async _execute() {

          const translated =
            pgCompatTranslateSql(
              this.sql
            );


          if (
            translated.skip
          ) {

            return {
              success:
                true,
              results:
                [],
              meta: {
                changes:
                  0,
                skipped:
                  true
              }
            };

          }


          const result =
            await client.query(
              translated.sql,
              this.params
            );


          return pgCompatResult(
            result
          );

        },


        async all() {

          return await this._execute();

        },


        async first() {

          const result =
            await this._execute();


          return (
            result.results?.[0] ||
            null
          );

        },


        async run() {

          return await this._execute();

        }

      };

    },


    async batch(
      statements
    ) {

      const results = [];


      for (
        const statement
        of (
          statements ||
          []
        )
      ) {

        if (
          !statement ||
          typeof statement._execute !==
            "function"
        ) {

          throw new Error(
            "Statement incompatible en batch PostgreSQL."
          );

        }


        results.push(
          await statement._execute()
        );

      }


      return results;

    },


    async bulkInsertJson(
      table,
      columns,
      rows,
      {
        replace = false,
        maxBytes = 220000
      } = {}
    ) {

      if (
        !rows ||
        !rows.length
      ) {

        return {
          rows:
            0,
          queries:
            0,
          chunks:
            0
        };

      }


      const primaryKeys =
        PG_COMPAT_PRIMARY_KEYS[
          table
        ];


      if (
        !primaryKeys
      ) {

        throw new Error(
          "Tabla sin PK registrada para PostgreSQL: " +
          table
        );

      }


      const chunks =
        jsonChunksByBytes(
          rows,
          Math.min(
            Number(
              maxBytes ||
              220000
            ),
            180000
          )
        );


      const tableSql =
        pgCompatQuoteIdent(
          table
        );


      const columnSql =
        columns
          .map(
            pgCompatQuoteIdent
          )
          .join(
            ", "
          );


      const selectSql =
        columns
          .map(
            pgCompatQuoteIdent
          )
          .join(
            ", "
          );


      let conflictSql =
        "";


      if (replace) {

        const conflictColumns =
          primaryKeys
            .map(
              pgCompatQuoteIdent
            )
            .join(
              ", "
            );


        const updateColumns =
          columns.filter(
            column =>
              !primaryKeys.includes(
                column
              )
          );


        if (
          updateColumns.length
        ) {

          conflictSql =
            "\nON CONFLICT (" +
            conflictColumns +
            ") DO UPDATE SET\n  " +
            updateColumns
              .map(
                column => {

                  const quoted =
                    pgCompatQuoteIdent(
                      column
                    );


                  return (
                    quoted +
                    " = EXCLUDED." +
                    quoted
                  );

                }
              )
              .join(
                ",\n  "
              );

        } else {

          conflictSql =
            "\nON CONFLICT (" +
            conflictColumns +
            ") DO NOTHING";

        }

      }


      const sql =
`INSERT INTO super_dupaza.${tableSql} (
  ${columnSql}
)
SELECT
  ${selectSql}
FROM jsonb_populate_recordset(
  NULL::super_dupaza.${tableSql},
  $1::jsonb
)
${conflictSql}`;


      let queries =
        0;


      for (
        const chunk
        of chunks
      ) {

        await client.query(
          sql,
          [
            JSON.stringify(
              chunk
            )
          ]
        );


        queries++;

      }


      return {
        rows:
          rows.length,

        queries,

        chunks:
          chunks.length
      };

    }

  };


  return compat;

}



async function postgresSyncSnapshot(
  env
) {

  return withPg(
    env,
    async client => {

      const counts =
        await client.query(
`SELECT
  (SELECT COUNT(*)::bigint FROM super_dupaza.zas_orders)
    AS zas_orders,

  (SELECT COUNT(*)::bigint FROM super_dupaza.zas_order_lines)
    AS zas_order_lines,

  (SELECT COUNT(*)::bigint FROM super_dupaza.loaded_refs)
    AS loaded_refs,

  (SELECT COUNT(*)::bigint FROM super_dupaza.report_orders)
    AS report_orders`
        );


      const states =
        await client.query(
`SELECT
  source,
  last_started_at,
  last_success_at,
  range_from,
  range_to,
  records_seen,
  records_changed,
  metadata_json,
  last_error
FROM super_dupaza.sync_state
WHERE source IN (
  'ZASCONNECT',
  'BO_VT',
  'REPORTS',
  'SCHEDULER'
)
ORDER BY source`
        );


      const row =
        counts.rows[0] ||
        {};


      return {
        ok:
          true,

        engine:
          "POSTGRESQL",

        counts: {
          zas_orders:
            Number(
              row.zas_orders ||
              0
            ),

          zas_order_lines:
            Number(
              row.zas_order_lines ||
              0
            ),

          loaded_refs:
            Number(
              row.loaded_refs ||
              0
            ),

          report_orders:
            Number(
              row.report_orders ||
              0
            )
        },

        syncState:
          states.rows
      };

    }
  );

}



async function syncIncrementalToPostgres(
  env,
  days = 7
) {

  if (
    !env.HYPERDRIVE?.connectionString
  ) {

    throw new Error(
      "Falta el binding HYPERDRIVE."
    );

  }


  const requestedDays =
    Math.min(
      30,
      Math.max(
        1,
        Number(
          days ||
          7
        ) ||
        7
      )
    );


  return withPg(
    env,
    async client => {

      await client.query(
        "BEGIN"
      );


      try {

        const pgEnv = {
          ...env,

          DB:
            makePgD1Compat(
              client
            )
        };


        const result =
          await syncIncrementalToD1(
            pgEnv,
            requestedDays
          );


        await client.query(
          "COMMIT"
        );


        const snapshot =
          await postgresSyncSnapshot(
            env
          );


        return {
          ok:
            true,

          engine:
            "POSTGRESQL",

          via:
            "HYPERDRIVE",

          transactionCommitted:
            true,

          queueSwitched:
            false,

          d1Modified:
            false,

          sync:
            result,

          snapshot
        };


      } catch (error) {

        try {

          await client.query(
            "ROLLBACK"
          );

        } catch {}


        throw error;

      }

    }
  );

}



async function syncIncrementalToD1(
  env,
  days = 7
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const startedAt =
    new Date()
      .toISOString();


  const now =
    new Date()
      .toISOString();


  const stats = {
    mode:
      "INCREMENTAL",

    days,

    startedAt,

    zasConnect: {
      apiLines:
        0,
      ordersSeen:
        0,
      changedOrders:
        0,
      deletedOrders:
        0,
      changedLineOrders:
        0
    },

    boVt: {
      sourceChanged:
        false,
      changed:
        0,
      deleted:
        0
    },

    crossSource: {
      checked:
        0,
      fullApiFetched:
        false,
      staleApiDeleted:
        0,
      kept:
        0
    },

    reports: {
      seen:
        0,
      changed:
        0,
      deleted:
        0,
      driftDetected:
        0,
      forcedRebuild:
        false
    },

    d1QueriesEstimated:
      0
  };


  try {

    // Tracking importados manualmente como API no deben borrarse solo
    // porque el export actual de ZasConnect los omita.
    const manualProtectedTrackings =
      await manualApiProtectedTrackings(
        env
      );


    // --------------------------------------------------------
    // ZASCONNECT: ventana reciente, no toda la historia.
    // --------------------------------------------------------

    const recent =
      await getZasOrdersRecent(
        env,
        days
      );


    const grouped =
      groupZasForD1(
        recent.rows
      );


    stats.zasConnect.apiLines =
      recent.rows.length;


    stats.zasConnect.ordersSeen =
      grouped.orders.length;


    const [
      existingOrdersResult,
      existingLinesResult
    ] =
      await env.DB.batch([

        env.DB
          .prepare(
`SELECT
  tracking_code,
  source_hash
FROM zas_orders
WHERE order_date >= ?1`
          )
          .bind(
            recent.cutoff
          ),

        env.DB
          .prepare(
`SELECT
  l.tracking_code,
  l.line_key,
  l.source_hash
FROM zas_order_lines AS l
JOIN zas_orders AS o
  ON o.tracking_code = l.tracking_code
WHERE o.order_date >= ?1`
          )
          .bind(
            recent.cutoff
          )

      ]);


    stats.d1QueriesEstimated +=
      2;


    const orderHashes =
      new Map(
        (
          existingOrdersResult?.results ||
          []
        )
          .map(
            row => [
              row.tracking_code,
              row.source_hash
            ]
          )
      );


    const lineHashes =
      new Map();


    for (
      const row
      of (
        existingLinesResult?.results ||
        []
      )
    ) {

      if (
        !lineHashes.has(
          row.tracking_code
        )
      ) {

        lineHashes.set(
          row.tracking_code,
          new Map()
        );

      }


      lineHashes
        .get(
          row.tracking_code
        )
        .set(
          row.line_key,
          row.source_hash
        );

    }


    const changedOrders =
      grouped.orders.filter(
        row =>
          orderHashes.get(
            row.tracking_code
          ) !==
          row.source_hash
      );


    // ZasConnect actual es la fuente de verdad dentro de la ventana reciente.
    // Si una orden que estaba guardada en D1 ya no viene en el API actual,
    // debe eliminarse de zas_orders/zas_order_lines. De lo contrario una venta
    // cancelada o retirada del API puede seguir apropiándose de un tracking
    // BO/VT y terminar sumándose al asesor equivocado.
    const incomingOrderTrackings =
      new Set(
        grouped.orders.map(
          row =>
            row.tracking_code
        )
      );


    const deletedOrders =
      [
        ...orderHashes.keys()
      ]
        .filter(
          tracking =>
            !incomingOrderTrackings.has(
              tracking
            ) &&
            !manualProtectedTrackings.has(
              normId(
                tracking
              )
            )
        );


    await markManualTrackingsSeenInApi(
      env,
      grouped.orders.map(
        row =>
          row.tracking_code
      ),
      now
    );


    const incomingLines =
      new Map();


    for (
      const row
      of grouped.lines
    ) {

      if (
        !incomingLines.has(
          row.tracking_code
        )
      ) {

        incomingLines.set(
          row.tracking_code,
          []
        );

      }


      incomingLines
        .get(
          row.tracking_code
        )
        .push(
          row
        );

    }


    const changedLineOrders =
      new Set();


    for (
      const order
      of grouped.orders
    ) {

      const tracking =
        order.tracking_code;


      const incoming =
        new Map(
          (
            incomingLines.get(
              tracking
            ) ||
            []
          )
            .map(
              row => [
                row.line_key,
                row.source_hash
              ]
            )
        );


      const existing =
        lineHashes.get(
          tracking
        ) ||
        new Map();


      if (
        incoming.size !==
        existing.size
      ) {

        changedLineOrders.add(
          tracking
        );

        continue;

      }


      for (
        const [
          key,
          hash
        ]
        of incoming.entries()
      ) {

        if (
          existing.get(
            key
          ) !==
          hash
        ) {

          changedLineOrders.add(
            tracking
          );

          break;

        }

      }

    }


    if (
      deletedOrders.length
    ) {

      // Primero líneas y luego cabeceras.
      stats.d1QueriesEstimated +=
        await d1DeleteTrackingChunks(
          env.DB,
          "zas_order_lines",
          deletedOrders
        );


      stats.d1QueriesEstimated +=
        await d1DeleteTrackingChunks(
          env.DB,
          "zas_orders",
          deletedOrders
        );

    }


    const orderWrite =
      await d1BulkInsertJson(
        env.DB,
        "zas_orders",
        [
          "tracking_code",
          "customer_tracking_code",
          "tax_id",
          "customer_name",
          "primary_phone",
          "secondary_phone",
          "seller_name",
          "shipping_municipality",
          "shipping_reference",
          "custom_rcf",
          "sales_channel",
          "shipping_department",
          "order_date",
          "comments",
          "warehouse",
          "order_total",
          "shipping_avenue",
          "shipping_house_number",
          "shipping_zone",
          "custom_bodega",
          "custom_canal",
          "custom_payment_method",
          "custom_marca",
          "source_hash",
          "source_updated_at",
          "synced_at"
        ],
        changedOrders.map(
          row => ({
            ...row,
            synced_at:
              now
          })
        ),
        {
          replace:
            true
        }
      );


    stats.d1QueriesEstimated +=
      orderWrite.queries;


    stats.zasConnect.changedOrders =
      changedOrders.length;


    stats.zasConnect.deletedOrders =
      deletedOrders.length;


    stats.zasConnect.changedLineOrders =
      changedLineOrders.size;


    if (
      changedLineOrders.size
    ) {

      const trackingList =
        [
          ...changedLineOrders
        ];


      stats.d1QueriesEstimated +=
        await d1DeleteTrackingChunks(
          env.DB,
          "zas_order_lines",
          trackingList
        );


      const linesToWrite =
        grouped.lines.filter(
          row =>
            changedLineOrders.has(
              row.tracking_code
            )
        );


      const lineWrite =
        await d1BulkInsertJson(
          env.DB,
          "zas_order_lines",
          [
            "tracking_code",
            "line_key",
            "line_number",
            "sku",
            "description",
            "quantity",
            "brand",
            "location_code",
            "ring_code",
            "unit_price",
            "line_total",
            "source_hash",
            "synced_at"
          ],
          linesToWrite.map(
            row => ({
              ...row,
              synced_at:
                now
            })
          ),
          {
            replace:
              true
          }
        );


      stats.d1QueriesEstimated +=
        lineWrite.queries;

    }


    // --------------------------------------------------------
    // BO / VT: solo bajar todo si el origen realmente cambió.
    // --------------------------------------------------------

    const [
      appsStatus,
      boState
    ] =
      await Promise.all([

        fetchAppsScript(
          env,
          "status"
        ),

        env.DB
          .prepare(
`SELECT
  metadata_json
FROM sync_state
WHERE source = 'BO_VT'`
          )
          .first()

      ]);


    stats.d1QueriesEstimated +=
      1;


    let previousMeta = {};


    try {

      previousMeta =
        JSON.parse(
          boState?.metadata_json ||
          "{}"
        );

    } catch {

      previousMeta = {};

    }


    const sourceKey =
      appsStatus.sourceModified ||
      appsStatus.lastImport ||
      null;


    const previousKey =
      previousMeta.sourceModified ||
      previousMeta.lastImport ||
      null;


    const needsBlankPdvBackfill =
      previousMeta.pdvBlankTrackingSupport !==
      1;


    const needsPdvAdvisorIdentityBackfill =
      previousMeta.pdvAdvisorIdentitySupport !==
      1;


    const needsPhysicalSegmentBackfill =
      previousMeta.manualPhysicalSegmentSupport !==
      1;


    const needsStaleSourceCleanup =
      previousMeta.staleSourceCleanupV1 !==
      1;


    const needsBoVtPdvBothSourcesBackfill =
      previousMeta.boVtPdvBothSourcesV1 !==
      1;


    if (
      !previousKey ||
      sourceKey !==
      previousKey ||
      needsBlankPdvBackfill ||
      needsPdvAdvisorIdentityBackfill ||
      needsPhysicalSegmentBackfill ||
      needsStaleSourceCleanup ||
      needsBoVtPdvBothSourcesBackfill
    ) {

      stats.boVt.sourceChanged =
        true;


      const apps =
        await fetchAppsScript(
          env,
          "all"
        );


      const bo =
        Array.isArray(
          apps.bo
        )
          ? apps.bo
          : [];


      const vt =
        Array.isArray(
          apps.vt
        )
          ? apps.vt
          : [];


      const physicalSellerSegments =
        await getPhysicalSellerSegmentMap(
          env
        );


      const detail =
        buildDetail(
          bo,
          vt,
          physicalSellerSegments
        );


      const desiredRefs =
        buildLoadedRefsForD1(
          detail
        );


      const existingRefsResult =
        await env.DB
          .prepare(
`SELECT
  tracking_code,
  source_hash
FROM loaded_refs`
          )
          .all();


      stats.d1QueriesEstimated +=
        1;


      const existingRefs =
        new Map(
          (
            existingRefsResult?.results ||
            []
          )
            .map(
              row => [
                row.tracking_code,
                row.source_hash
              ]
            )
        );


      const desiredMap =
        new Map(
          desiredRefs.map(
            row => [
              row.tracking_code,
              row
            ]
          )
        );


      const changedRefs =
        desiredRefs.filter(
          row =>
            existingRefs.get(
              row.tracking_code
            ) !==
            row.source_hash
        );


      const deletedRefs =
        [
          ...existingRefs.keys()
        ]
          .filter(
            tracking =>
              !desiredMap.has(
                tracking
              )
          );


      if (
        deletedRefs.length
      ) {

        stats.d1QueriesEstimated +=
          await d1DeleteTrackingChunks(
            env.DB,
            "loaded_refs",
            deletedRefs
          );

      }


      const refWrite =
        await d1BulkInsertJson(
          env.DB,
          "loaded_refs",
          [
            "tracking_code",
            "is_loaded",
            "is_pdv",
            "has_bo",
            "has_vt",
            "report_date",
            "route",
            "channel",
            "group_name",
            "bo_amount",
            "vt_amount",
            "chosen_amount",
            "source_hash",
            "synced_at"
          ],
          changedRefs.map(
            row => ({
              ...row,
              synced_at:
                now
            })
          ),
          {
            replace:
              true
          }
        );


      stats.d1QueriesEstimated +=
        refWrite.queries;


      stats.boVt.changed =
        changedRefs.length;


      stats.boVt.deleted =
        deletedRefs.length;


      const syntheticPdvRefs =
        desiredRefs.filter(
          row =>
            String(
              row.tracking_code ||
              ""
            )
              .startsWith(
                "PDV-NOID|"
              )
        )
          .length;


      previousMeta = {
        lastImport:
          apps.lastImport ||
          appsStatus.lastImport ||
          null,

        sourceModified:
          apps.sourceModified ||
          appsStatus.sourceModified ||
          null,

        boRows:
          bo.length,

        vtRows:
          vt.length,

        loadedRefs:
          desiredRefs.length,

        pdvBlankTrackingSupport:
          1,

        pdvAdvisorIdentitySupport:
          1,

        manualPhysicalSegmentSupport:
          1,

        staleSourceCleanupV1:
          1,

        // BO y VT participan en PDV. Tracking real usa ZasConnect;
        // sin tracking cada documento conserva identidad propia y
        // los pares BO/VT verificables se deduplican.
        boVtPdvBothSourcesV1:
          1,

        pendingNormalizationDoesNotRenameReports:
          1,

        syntheticPdvRefs
      };

    }


    // --------------------------------------------------------
    // RECONCILIACIÓN CRUZADA ZASCONNECT ↔ BO/VT
    //
    // Caso importante:
    // una fila de zas_orders puede tener una order_date vieja y por eso
    // quedar fuera del cleanup reciente, pero loaded_refs puede volver a
    // usar el mismo tracking en una fecha BO/VT reciente.
    //
    // Verificamos esos trackings contra el API COMPLETO actual antes de
    // decidir si la fila ZasConnect en D1 sigue siendo válida.
    // --------------------------------------------------------

    const crossSource =
      await reconcileRecentLoadedRefsAgainstLiveApi(
        env,
        recent.cutoff
      );


    stats.crossSource = {
      checked:
        crossSource.checked ||
        0,

      fullApiFetched:
        Boolean(
          crossSource.fullApiFetched
        ),

      staleApiDeleted:
        crossSource.deleted?.length ||
        0,

      kept:
        crossSource.kept?.length ||
        0
    };


    stats.d1QueriesEstimated +=
      1 +
      (
        crossSource.deleteQueries ||
        0
      );


    // --------------------------------------------------------
    // REPORT_ORDERS: solo reconciliar si cambió cabecera o BO/VT.
    // --------------------------------------------------------

    const advisorSeparationState =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_ADVISOR_SEPARATION_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const identityV2State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_ADVISOR_IDENTITY_V2'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsAdvisorSeparationRebuild =
      !advisorSeparationState;


    const reportMaterializationV3State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_MATERIALIZATION_V3'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsAdvisorIdentityV2Rebuild =
      !identityV2State;


    const pdvAttributionV1State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_PDV_ATTRIBUTION_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsReportMaterializationV3Rebuild =
      !reportMaterializationV3State;


    const driftPdvFixV1State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_DRIFT_PDV_FIX_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsPdvAttributionV1Rebuild =
      !pdvAttributionV1State;


    const reportDateSourceV1State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_DATE_SOURCE_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsDriftPdvFixV1Rebuild =
      !driftPdvFixV1State;


    const trackedApiOwnerV1State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_TRACKED_API_OWNER_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsReportDateSourceV1Rebuild =
      !reportDateSourceV1State;


    const trackedSellerSegmentV1State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_TRACKED_SELLER_SEGMENT_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsTrackedApiOwnerV1Rebuild =
      !trackedApiOwnerV1State;


    const needsTrackedSellerSegmentV1Rebuild =
      !trackedSellerSegmentV1State;


    const boVtPdvBothSourcesV1State =
      await env.DB
        .prepare(
`SELECT source
FROM sync_state
WHERE source = 'REPORT_BO_VT_PDV_BOTH_SOURCES_V1'`
        )
        .first();


    stats.d1QueriesEstimated +=
      1;


    const needsBoVtPdvBothSourcesV1Rebuild =
      !boVtPdvBothSourcesV1State;


    const reportDrift =
      await detectReportMaterializationDrift(
        env
      );


    stats.d1QueriesEstimated +=
      1;


    stats.reports.driftDetected =
      reportDrift;


    const sourcesChanged =
      changedOrders.length >
        0 ||
      deletedOrders.length >
        0 ||
      stats.crossSource.staleApiDeleted >
        0 ||
      stats.boVt.changed >
        0 ||
      stats.boVt.deleted >
        0 ||
      needsAdvisorSeparationRebuild ||
      needsAdvisorIdentityV2Rebuild ||
      needsReportMaterializationV3Rebuild ||
      needsPdvAttributionV1Rebuild ||
      needsDriftPdvFixV1Rebuild ||
      needsReportDateSourceV1Rebuild ||
      needsTrackedApiOwnerV1Rebuild ||
      needsTrackedSellerSegmentV1Rebuild ||
      needsBoVtPdvBothSourcesV1Rebuild ||
      reportDrift >
        0;


    let reportStats;


    if (
      sourcesChanged
    ) {

      stats.reports.forcedRebuild =
        Boolean(
          needsReportMaterializationV3Rebuild ||
          needsPdvAttributionV1Rebuild ||
          needsDriftPdvFixV1Rebuild ||
          needsReportDateSourceV1Rebuild ||
          needsTrackedApiOwnerV1Rebuild ||
          needsTrackedSellerSegmentV1Rebuild ||
          needsBoVtPdvBothSourcesV1Rebuild ||
          reportDrift >
            0
        );


      reportStats =
        await rebuildReportOrdersFromD1(
          env,
          now
        );


      stats.d1QueriesEstimated +=
        3 +
        reportStats.writeQueries +
        reportStats.deleteQueries;


      if (
        needsAdvisorSeparationRebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_ADVISOR_SEPARATION_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              separated:
                true,
              juan:
                "JUAN MEJIA",
              glenda:
                "GLENDA REINOSO"
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsAdvisorIdentityV2Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_ADVISOR_IDENTITY_V2',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              advisorIdentityPreserved:
                true,
              pendingCanonicalizationOnlyInPendingView:
                true,
              classificationFromNormalizationRows:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsReportMaterializationV3Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_MATERIALIZATION_V3',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              materializedFromCurrentSources:
                true,
              staleApiRowsRemoved:
                true,
              pdvBoVtRecovered:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsPdvAttributionV1Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_PDV_ATTRIBUTION_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              sameDayApiKeepsSeller:
                true,
              laterPdvDayUsesBoVtAdvisor:
                true,
              laterPdvDayUsesBoVtAmount:
                true,
              laterPdvDayUsesBoVtSource:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsDriftPdvFixV1Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_DRIFT_PDV_FIX_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              validCrossSourcePdvExcludedFromGenericDrift:
                true,
              overlapIsNotCollision:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsReportDateSourceV1Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_DATE_SOURCE_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              uidFrontUsesZasConnectDate:
                true,
              blankUidUsesBoVtDate:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsTrackedApiOwnerV1Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_TRACKED_API_OWNER_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              trackedOrderAdvisorFromZasConnect:
                true,
              trackedOrderAmountFromZasConnect:
                true,
              trackedOrderDateFromZasConnect:
                true,
              trackedOrderSourceZasConnect:
                true,
              boVtOnlyAddsPhysicalSegmentForTrackedOrders:
                true,
              blankUidOwnedByBoVt:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsTrackedSellerSegmentV1Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_TRACKED_SELLER_SEGMENT_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              trackedOrderClassificationFromApiSellerRule:
                true,
              boVtDoesNotOverrideTrackedSellerSegment:
                true,
              pdvNoUidStillUsesBoVtPhysicalSegment:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }


      if (
        needsBoVtPdvBothSourcesV1Rebuild
      ) {

        await env.DB
          .prepare(
`INSERT OR IGNORE INTO sync_state (
  source,
  last_success_at,
  records_seen,
  records_changed,
  metadata_json
)
VALUES (
  'REPORT_BO_VT_PDV_BOTH_SOURCES_V1',
  ?1,
  ?2,
  ?3,
  ?4
)`
          )
          .bind(
            now,
            reportStats.seen,
            reportStats.changed,
            JSON.stringify({
              boPdvParticipates:
                true,
              vtPdvParticipates:
                true,
              trackedOrdersOwnedByApi:
                true,
              blankBoUsesFechaCreado:
                true,
              blankVtUsesVtDate:
                true,
              sameTrackingCountedOnce:
                true,
              verifiableBlankBoVtPairCountedOnce:
                true
            })
          )
          .run();


        stats.d1QueriesEstimated +=
          1;

      }

    } else {

      const state =
        await env.DB
          .prepare(
`SELECT
  records_seen,
  range_from,
  range_to
FROM sync_state
WHERE source = 'REPORTS'`
          )
          .first();


      stats.d1QueriesEstimated +=
        1;


      reportStats = {
        seen:
          Number(
            state?.records_seen ||
            0
          ),
        changed:
          0,
        deleted:
          0,
        rangeFrom:
          state?.range_from ||
          null,
        rangeTo:
          state?.range_to ||
          null,
        writeQueries:
          0,
        deleteQueries:
          0
      };

    }


    stats.reports.seen =
      reportStats.seen;

    stats.reports.changed =
      reportStats.changed;

    stats.reports.deleted =
      reportStats.deleted;


    const zasMeta =
      JSON.stringify({
        days,
        cutoff:
          recent.cutoff,
        apiLines:
          recent.rows.length,
        ordersSeen:
          grouped.orders.length,
        changedOrders:
          changedOrders.length,

        deletedOrders:
          deletedOrders.length,

        changedLineOrders:
          changedLineOrders.size
      });


    const boMeta =
      JSON.stringify(
        previousMeta
      );


    const reportMeta =
      JSON.stringify({
        reportOrders:
          reportStats.seen,

        changed:
          reportStats.changed,

        deleted:
          reportStats.deleted,

        crossSource:
          stats.crossSource
      });


    await env.DB.batch([

      env.DB
        .prepare(
`UPDATE sync_state
 SET last_started_at = ?1,
     last_success_at = ?2,
     range_from = ?3,
     range_to = ?4,
     records_seen = ?5,
     records_changed = ?6,
     metadata_json = ?7,
     last_error = NULL
 WHERE source = 'ZASCONNECT'`
        )
        .bind(
          startedAt,
          now,
          recent.cutoff,
          gtDateOnly(),
          grouped.orders.length,
          changedOrders.length +
          deletedOrders.length,
          zasMeta
        ),

      env.DB
        .prepare(
`UPDATE sync_state
 SET last_started_at = ?1,
     last_success_at = ?2,
     records_changed = ?3,
     metadata_json = ?4,
     last_error = NULL
 WHERE source = 'BO_VT'`
        )
        .bind(
          startedAt,
          now,
          stats.boVt.changed,
          boMeta
        ),

      env.DB
        .prepare(
`UPDATE sync_state
 SET last_started_at = ?1,
     last_success_at = ?2,
     range_from = ?3,
     range_to = ?4,
     records_seen = ?5,
     records_changed = ?6,
     metadata_json = ?7,
     last_error = NULL
 WHERE source = 'REPORTS'`
        )
        .bind(
          startedAt,
          now,
          reportStats.rangeFrom,
          reportStats.rangeTo,
          reportStats.seen,
          reportStats.changed,
          reportMeta
        )

    ]);


    stats.d1QueriesEstimated +=
      3;


    stats.finishedAt =
      new Date()
        .toISOString();


    return {
      ok: true,
      ...stats
    };


  } catch (error) {

    const message =
      String(
        error?.stack ||
        error
      )
        .slice(
          0,
          4000
        );


    try {

      await env.DB
        .prepare(
`UPDATE sync_state
 SET last_error = ?1,
     last_started_at = ?2
 WHERE source IN ('ZASCONNECT','BO_VT','REPORTS')`
        )
        .bind(
          message,
          startedAt
        )
        .run();

    } catch {
      // No ocultar el error original.
    }


    throw error;

  }

}



// ============================================================
// EXPORTADOR D1 DEFINITIVO -> JSON
// Basado en el inventario real de producción del 2026-10-27.
// ============================================================

const D1_FINAL_EXPORT_TABLES = [{"name": "loaded_refs", "expected": 7031}, {"name": "manual_sap_control", "expected": 41}, {"name": "manual_zas_api_imports", "expected": 2}, {"name": "manual_zas_api_registry", "expected": 59}, {"name": "pending_name_normalization", "expected": 25}, {"name": "report_group_labels", "expected": 3}, {"name": "report_orders", "expected": 2435}, {"name": "report_type_config", "expected": 1}, {"name": "report_type_value_rules", "expected": 3}, {"name": "seller_monthly_targets", "expected": 11}, {"name": "seller_report_match", "expected": 55}, {"name": "seller_report_rule_rows", "expected": 25}, {"name": "sync_state", "expected": 18}, {"name": "zas_order_lines", "expected": 1200}, {"name": "zas_orders", "expected": 430}];


async function d1FinalExportPageData(
  env,
  params
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 DB."
    );

  }


  const table =
    String(
      params.get(
        "table"
      ) ||
      ""
    )
      .trim();


  const allowed =
    D1_FINAL_EXPORT_TABLES
      .find(
        item =>
          item.name ===
          table
      );


  if (!allowed) {

    throw new Error(
      "Tabla no permitida."
    );

  }


  const cursor =
    Math.max(
      0,
      Number(
        params.get(
          "cursor"
        ) ||
        0
      ) ||
      0
    );


  const limit =
    Math.min(
      1000,
      Math.max(
        1,
        Number(
          params.get(
            "limit"
          ) ||
          1000
        ) ||
        1000
      )
    );


  const safeTable =
    table.replace(
      /"/g,
      '""'
    );


  // Keyset pagination por rowid: evita el coste creciente de OFFSET.
  const result =
    await env.DB
      .prepare(
        `SELECT
           rowid AS __d1_rowid__,
           *
         FROM "${safeTable}"
         WHERE rowid > ?1
         ORDER BY rowid
         LIMIT ?2`
      )
      .bind(
        cursor,
        limit
      )
      .all();


  const rawRows =
    result?.results ||
    [];


  let nextCursor =
    cursor;


  const rows =
    rawRows.map(
      raw => {

        const row = {
          ...raw
        };


        nextCursor =
          Math.max(
            nextCursor,
            Number(
              row.__d1_rowid__ ||
              0
            )
          );


        delete row.__d1_rowid__;


        return row;

      }
    );


  return {
    ok:
      true,

    source:
      "D1",

    table,

    expected:
      allowed.expected,

    cursor,

    nextCursor,

    returned:
      rows.length,

    done:
      rows.length <
      limit,

    rows
  };

}


function d1FinalExportPage() {

  return new Response(
    String.raw`<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">

  <meta
    name="viewport"
    content="width=device-width,initial-scale=1"
  >

  <title>
    SUPER DUPAZA · Exportación final D1
  </title>

  <style>
    *{box-sizing:border-box}
    body{
      margin:0;
      background:#f4f6f8;
      color:#101828;
      font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    }
    .wrap{
      max-width:980px;
      margin:36px auto;
      padding:0 18px;
    }
    .card{
      background:#fff;
      border:1px solid #d0d5dd;
      border-radius:16px;
      padding:22px;
      box-shadow:0 8px 28px rgba(16,24,40,.06);
    }
    h1{
      margin:0 0 8px;
      font-size:26px;
    }
    p{
      color:#475467;
      line-height:1.5;
    }
    .note{
      padding:12px 14px;
      border-radius:10px;
      margin:14px 0;
      background:#ecfdf3;
      border:1px solid #abefc6;
      color:#067647;
      font-weight:700;
    }
    label{
      display:block;
      margin:18px 0 6px;
      font-size:13px;
      font-weight:800;
    }
    input{
      width:100%;
      padding:11px 12px;
      border:1px solid #98a2b3;
      border-radius:9px;
      font:inherit;
    }
    button{
      margin-top:14px;
      border:0;
      border-radius:9px;
      padding:11px 16px;
      font-weight:900;
      cursor:pointer;
      background:#111827;
      color:#fff;
    }
    button:disabled{
      opacity:.45;
      cursor:not-allowed;
    }
    pre{
      margin-top:18px;
      padding:14px;
      border:1px solid #e4e7ec;
      border-radius:10px;
      background:#f9fafb;
      white-space:pre-wrap;
      overflow-wrap:anywhere;
      min-height:180px;
      font-size:12px;
    }
    .ok{color:#067647}
    .bad{color:#b42318}
  </style>
</head>

<body>

  <main class="wrap">

    <section class="card">

      <h1>
        Exportación final D1 → Supabase
      </h1>

      <p>
        Exporta únicamente las 15 tablas que existen realmente
        en SUPER DUPAZA.
      </p>

      <div class="note">
        Snapshot esperado: 11339 filas.
        El exportador verifica el conteo de cada tabla antes de descargar el JSON.
      </div>

      <label for="token">
        D1_SYNC_TOKEN
      </label>

      <input
        id="token"
        type="password"
        autocomplete="off"
        placeholder="Pega tu token administrativo"
      >

      <button id="exportBtn">
        EXPORTAR SNAPSHOT FINAL
      </button>

      <pre id="progress">
Esperando exportación...
      </pre>

    </section>

  </main>


<script>

const TABLES =
  [{"name": "loaded_refs", "expected": 7031}, {"name": "manual_sap_control", "expected": 41}, {"name": "manual_zas_api_imports", "expected": 2}, {"name": "manual_zas_api_registry", "expected": 59}, {"name": "pending_name_normalization", "expected": 25}, {"name": "report_group_labels", "expected": 3}, {"name": "report_orders", "expected": 2435}, {"name": "report_type_config", "expected": 1}, {"name": "report_type_value_rules", "expected": 3}, {"name": "seller_monthly_targets", "expected": 11}, {"name": "seller_report_match", "expected": 55}, {"name": "seller_report_rule_rows", "expected": 25}, {"name": "sync_state", "expected": 18}, {"name": "zas_order_lines", "expected": 1200}, {"name": "zas_orders", "expected": 430}];

const PAGE_SIZE =
  1000;

const EXPECTED_TOTAL =
  11339;


const tokenInput =
  document.getElementById(
    "token"
  );

const exportBtn =
  document.getElementById(
    "exportBtn"
  );

const progress =
  document.getElementById(
    "progress"
  );


function log(
  message
) {

  progress.textContent +=
    (
      progress.textContent
        .trim()
        ? "\n"
        : ""
    ) +
    message;


  progress.scrollTop =
    progress.scrollHeight;

}


async function fetchTable(
  meta,
  token
) {

  const allRows = [];
  let cursor = 0;


  while (true) {

    const response =
      await fetch(
        "/api/d1/export-final-page?table=" +
        encodeURIComponent(
          meta.name
        ) +
        "&cursor=" +
        cursor +
        "&limit=" +
        PAGE_SIZE +
        "&ts=" +
        Date.now(),
        {
          cache:
            "no-store",

          headers: {
            "X-Super-Dupaza-Admin":
              token
          }
        }
      );


    const data =
      await response.json();


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        (
          "HTTP " +
          response.status
        )
      );

    }


    const rows =
      Array.isArray(
        data.rows
      )
        ? data.rows
        : [];


    allRows.push(
      ...rows
    );


    log(
      meta.name +
      ": " +
      allRows.length +
      " / " +
      meta.expected
    );


    if (
      data.done ||
      rows.length <
        PAGE_SIZE
    ) {

      break;

    }


    if (
      Number(
        data.nextCursor ||
        0
      ) <=
      cursor
    ) {

      throw new Error(
        "Cursor no avanzó en " +
        meta.name
      );

    }


    cursor =
      Number(
        data.nextCursor
      );

  }


  if (
    allRows.length !==
    meta.expected
  ) {

    throw new Error(
      meta.name +
      ": esperado " +
      meta.expected +
      ", exportado " +
      allRows.length
    );

  }


  return allRows;

}


function downloadJson(
  payload
) {

  const blob =
    new Blob(
      [
        JSON.stringify(
          payload
        )
      ],
      {
        type:
          "application/json"
      }
    );


  const link =
    document.createElement(
      "a"
    );


  link.href =
    URL.createObjectURL(
      blob
    );


  link.download =
    "super-dupaza-d1-final-" +
    new Date()
      .toISOString()
      .slice(
        0,
        10
      ) +
    ".json";


  document.body.appendChild(
    link
  );


  link.click();


  link.remove();


  setTimeout(
    () =>
      URL.revokeObjectURL(
        link.href
      ),
    30000
  );

}


exportBtn.addEventListener(
  "click",
  async () => {

    const token =
      tokenInput.value
        .trim();


    if (!token) {

      alert(
        "Escribe D1_SYNC_TOKEN."
      );

      return;

    }


    exportBtn.disabled =
      true;

    progress.textContent =
      "";


    try {

      const tables = {};
      const counts = {};

      let exportedTotal = 0;


      log(
        "Iniciando snapshot final..."
      );


      for (
        const meta
        of TABLES
      ) {

        log(
          "Leyendo " +
          meta.name +
          "..."
        );


        const rows =
          await fetchTable(
            meta,
            token
          );


        tables[
          meta.name
        ] =
          rows;


        counts[
          meta.name
        ] =
          rows.length;


        exportedTotal +=
          rows.length;


        log(
          "✓ " +
          meta.name +
          ": " +
          rows.length
        );

      }


      if (
        exportedTotal !==
        EXPECTED_TOTAL
      ) {

        throw new Error(
          "Conteo total distinto. Esperado " +
          EXPECTED_TOTAL +
          ", exportado " +
          exportedTotal
        );

      }


      const payload = {
        format:
          "SUPER_DUPAZA_D1_FINAL_V1",

        exportedAt:
          new Date()
            .toISOString(),

        source:
          "Cloudflare D1",

        destination:
          "Supabase PostgreSQL / super_dupaza",

        tableCount:
          TABLES.length,

        expectedTotal:
          EXPECTED_TOTAL,

        exportedTotal,

        verified:
          true,

        counts,

        tables
      };


      downloadJson(
        payload
      );


      log(
        "✓ SNAPSHOT VERIFICADO"
      );


      log(
        "✓ Total: " +
        exportedTotal +
        " filas"
      );


      log(
        "Archivo descargado. Súbelo al chat."
      );

    } catch (error) {

      console.error(
        error
      );


      log(
        "ERROR: " +
        String(
          error?.message ||
          error
        )
      );

    } finally {

      exportBtn.disabled =
        false;

    }

  }
);

</script>

</body>
</html>`,
    {
      headers: {
        "content-type":
          "text/html; charset=utf-8",

        "cache-control":
          "no-store"
      }
    }
  );

}





// ============================================================
// POSTGRES PRIMARY READS + DUAL CONTROL WRITES
// ============================================================

async function postgresWriteSmokeTest(
  env
) {

  return withPg(
    env,
    async client => {

      await client.query("BEGIN");


      try {

        const result =
          await client.query(
`UPDATE super_dupaza.report_group_labels
SET display_name = display_name
WHERE level = 'K1'
RETURNING level, display_name`
          );


        await client.query("ROLLBACK");


        return {
          ok: true,
          writePermission:
            true,
          rolledBack:
            true,
          rowsTouched:
            result.rowCount,
          sample:
            result.rows[0] ||
            null,
          note:
            "La prueba se revirtió con ROLLBACK; no modificó datos persistentes."
        };

      } catch (error) {

        try {
          await client.query("ROLLBACK");
        } catch {}


        return {
          ok: false,
          writePermission:
            false,
          rolledBack:
            true,
          code:
            error?.code ||
            null,
          error:
            error?.message ||
            String(error)
        };

      }

    }
  );

}



async function pgFirstRead(
  label,
  pgCallback,
  d1Callback
) {

  try {

    const result =
      await pgCallback();


    if (
      result &&
      typeof result === "object"
    ) {

      return {
        ...result,
        databasePrimary:
          "POSTGRESQL",
        fallbackUsed:
          false
      };

    }


    return result;

  } catch (error) {

    console.error(
      "PG_FIRST_READ_FALLBACK",
      label,
      error?.message ||
      String(error)
    );


    const fallback =
      await d1Callback();


    if (
      fallback &&
      typeof fallback === "object"
    ) {

      return {
        ...fallback,
        databasePrimary:
          "D1_FALLBACK",
        fallbackUsed:
          true,
        postgresError:
          error?.message ||
          String(error)
      };

    }


    return fallback;

  }

}

async function pgOnlyRead(
  label,
  postgresLoader
) {

  const result =
    await postgresLoader();


  return {
    ...result,

    databasePrimary:
      "POSTGRESQL",

    fallbackUsed:
      false,

    d1OperationalPath:
      false,

    readMode:
      "POSTGRESQL_ONLY",

    readLabel:
      label
  };

}



async function pgReportConfigSaveLabels(
  env,
  body
) {

  const incoming =
    body?.labels ||
    {};


  const values = {
    K1:
      String(
        incoming.K1 ??
        ""
      ).trim(),
    K2:
      String(
        incoming.K2 ??
        ""
      ).trim(),
    K3:
      String(
        incoming.K3 ??
        ""
      ).trim()
  };


  for (
    const level
    of [
      "K1",
      "K2",
      "K3"
    ]
  ) {

    if (!values[level]) {

      throw new Error(
        "Los nombres visuales K1, K2 y K3 son obligatorios."
      );

    }

  }


  const now =
    new Date()
      .toISOString();


  return withPg(
    env,
    async client => {

      await client.query(
        "BEGIN"
      );


      try {

        for (
          let index = 0;
          index < 3;
          index++
        ) {

          const level =
            [
              "K1",
              "K2",
              "K3"
            ][index];


          await client.query(
`INSERT INTO super_dupaza.report_group_labels (
  level,
  display_name,
  sort_order,
  updated_at
)
VALUES ($1,$2,$3,$4)
ON CONFLICT(level)
DO UPDATE SET
  display_name = EXCLUDED.display_name,
  sort_order = EXCLUDED.sort_order,
  updated_at = EXCLUDED.updated_at`,
            [
              level,
              values[level],
              index + 1,
              now
            ]
          );

        }


        await client.query(
          "COMMIT"
        );

      } catch (error) {

        await client.query(
          "ROLLBACK"
        );

        throw error;

      }


      return {
        ok: true,
        labels:
          values,
        message:
          "Nombres visuales guardados.",
        databasePrimary:
          "POSTGRESQL"
      };

    }
  );

}


async function dualReportConfigSaveLabels(
  env,
  body
) {

  const primary =
    await pgReportConfigSaveLabels(
      env,
      body
    );


  try {

    await reportConfigSaveLabels(
      env,
      body
    );

    return {
      ...primary,
      mirroredToD1:
        true
    };

  } catch (error) {

    console.error(
      "D1_MIRROR_REPORT_CONFIG_FAILED",
      error?.message ||
      String(error)
    );

    return {
      ...primary,
      mirroredToD1:
        false,
      mirrorWarning:
        error?.message ||
        String(error)
    };

  }

}


async function pgSellerTargetsSavePrimary(
  env,
  body
) {

  const month =
    validTargetMonth(
      body?.month
    );


  const incoming =
    Array.isArray(
      body?.targets
    )
      ? body.targets
      : [];


  const now =
    new Date()
      .toISOString();


  return withPg(
    env,
    async client => {

      await client.query(
        "BEGIN"
      );


      let saved = 0;


      try {

        for (
          const item
          of incoming
        ) {

          const seller =
            String(
              item?.seller ||
              ""
            ).trim();


          const sellerKey =
            normalizeMatchText(
              seller
            );


          const amount =
            Number(
              item?.amount ||
              0
            );


          if (
            !seller ||
            !sellerKey ||
            !Number.isFinite(amount) ||
            amount < 0
          ) {
            continue;
          }


          if (amount <= 0) {

            await client.query(
`DELETE FROM super_dupaza.seller_monthly_targets
WHERE seller_key = $1
  AND month = $2`,
              [
                sellerKey,
                month
              ]
            );

            saved++;
            continue;

          }


          await client.query(
`INSERT INTO super_dupaza.seller_monthly_targets (
  seller_key,
  seller_name,
  month,
  target_amount,
  updated_at
)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT(seller_key, month)
DO UPDATE SET
  seller_name = EXCLUDED.seller_name,
  target_amount = EXCLUDED.target_amount,
  updated_at = EXCLUDED.updated_at`,
            [
              sellerKey,
              seller,
              month,
              round2(amount),
              now
            ]
          );

          saved++;

        }


        await client.query(
          "COMMIT"
        );

      } catch (error) {

        await client.query(
          "ROLLBACK"
        );

        throw error;

      }


      return {
        ok: true,
        month,
        saved,
        message:
          "Metas mensuales guardadas.",
        databasePrimary:
          "POSTGRESQL"
      };

    }
  );

}


async function dualSellerTargetsSave(
  env,
  body
) {

  const primary =
    await pgSellerTargetsSavePrimary(
      env,
      body
    );


  try {

    await sellerTargetsSave(
      env,
      body
    );

    return {
      ...primary,
      mirroredToD1:
        true
    };

  } catch (error) {

    console.error(
      "D1_MIRROR_SELLER_TARGETS_FAILED",
      error?.message ||
      String(error)
    );

    return {
      ...primary,
      mirroredToD1:
        false,
      mirrorWarning:
        error?.message ||
        String(error)
    };

  }

}


async function pgManualSapMarkPrimary(
  env,
  trackingValue
) {

  const tracking =
    requireTrackingCode(
      trackingValue
    );


  const now =
    new Date()
      .toISOString();


  return withPg(
    env,
    async client => {

      const orderResult =
        await client.query(
`SELECT
  tracking_code,
  customer_name,
  seller_name,
  order_date,
  order_total
FROM super_dupaza.zas_orders
WHERE tracking_code = $1`,
          [
            tracking
          ]
        );


      const order =
        orderResult.rows[0];


      if (!order) {

        throw new Error(
          "La orden no existe en zas_orders."
        );

      }


      await client.query(
`INSERT INTO super_dupaza.manual_sap_control (
  tracking_code,
  status,
  marked_sap_at,
  resolved_at,
  updated_at,
  customer_name,
  seller_name,
  order_date,
  order_total
)
VALUES (
  $1,
  'EN_SAP',
  $2,
  NULL,
  $2,
  $3,
  $4,
  $5,
  $6
)
ON CONFLICT(tracking_code)
DO UPDATE SET
  status = 'EN_SAP',
  marked_sap_at = CASE
    WHEN super_dupaza.manual_sap_control.marked_sap_at IS NULL
    THEN EXCLUDED.marked_sap_at
    ELSE super_dupaza.manual_sap_control.marked_sap_at
  END,
  resolved_at = NULL,
  updated_at = EXCLUDED.updated_at,
  customer_name = EXCLUDED.customer_name,
  seller_name = EXCLUDED.seller_name,
  order_date = EXCLUDED.order_date,
  order_total = EXCLUDED.order_total`,
        [
          tracking,
          now,
          order.customer_name,
          order.seller_name,
          order.order_date,
          numberValue(
            order.order_total
          )
        ]
      );


      return {
        ok: true,
        trackingCode:
          tracking,
        status:
          "EN_SAP",
        message:
          "Orden movida a Control SAP.",
        databasePrimary:
          "POSTGRESQL"
      };

    }
  );

}


async function dualManualSapMark(
  env,
  trackingValue
) {

  const primary =
    await pgManualSapMarkPrimary(
      env,
      trackingValue
    );


  try {

    await manualSapMark(
      env,
      trackingValue
    );

    return {
      ...primary,
      mirroredToD1:
        true
    };

  } catch (error) {

    console.error(
      "D1_MIRROR_MANUAL_SAP_MARK_FAILED",
      error?.message ||
      String(error)
    );

    return {
      ...primary,
      mirroredToD1:
        false,
      mirrorWarning:
        error?.message ||
        String(error)
    };

  }

}


async function pgManualSapResolvePrimary(
  env,
  trackingValue
) {

  const tracking =
    requireTrackingCode(
      trackingValue
    );


  const now =
    new Date()
      .toISOString();


  return withPg(
    env,
    async client => {

      const result =
        await client.query(
`UPDATE super_dupaza.manual_sap_control
SET
  status = 'RESUELTO',
  resolved_at = $2,
  updated_at = $2
WHERE tracking_code = $1
RETURNING tracking_code`,
          [
            tracking,
            now
          ]
        );


      if (!result.rows.length) {

        throw new Error(
          "La orden no está en Control SAP."
        );

      }


      return {
        ok: true,
        trackingCode:
          tracking,
        status:
          "RESUELTO",
        virtualLoaded:
          true,
        message:
          "Orden resuelta. Se considera cargada aunque no exista físicamente en BO/VT.",
        databasePrimary:
          "POSTGRESQL"
      };

    }
  );

}


async function dualManualSapResolve(
  env,
  trackingValue
) {

  const primary =
    await pgManualSapResolvePrimary(
      env,
      trackingValue
    );


  try {

    await manualSapResolve(
      env,
      trackingValue
    );

    return {
      ...primary,
      mirroredToD1:
        true
    };

  } catch (error) {

    console.error(
      "D1_MIRROR_MANUAL_SAP_RESOLVE_FAILED",
      error?.message ||
      String(error)
    );

    return {
      ...primary,
      mirroredToD1:
        false,
      mirrorWarning:
        error?.message ||
        String(error)
    };

  }

}


async function pgRebuildSellerReportMatch(
  client
) {

  const result =
    await client.query(
`SELECT
  n.id,
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  c.k_level,
  c.segment,
  c.updated_at
FROM super_dupaza.pending_name_normalization AS n
JOIN super_dupaza.seller_report_rule_rows AS c
  ON c.normalization_id = n.id
WHERE c.k_level IS NOT NULL
   OR c.segment IS NOT NULL
ORDER BY n.id`
    );


  const rows =
    result.rows ||
    [];


  const canonicalCounts =
    new Map();


  for (const row of rows) {

    const key =
      normText(
        row.canonical_name
      );

    canonicalCounts.set(
      key,
      (
        canonicalCounts.get(key) ||
        0
      ) + 1
    );

  }


  const canonicalOwner =
    new Map();


  for (const row of rows) {

    const key =
      normText(
        row.canonical_name
      );


    if (
      canonicalCounts.get(key) === 1
    ) {

      canonicalOwner.set(
        key,
        Number(row.id)
      );

      continue;

    }


    const nonCanonicalAliases =
      [
        row.alias1,
        row.alias2,
        row.alias3,
        row.alias4,
        row.alias5
      ]
        .map(normText)
        .filter(
          value =>
            value &&
            value !== key
        );


    if (
      !canonicalOwner.has(key) &&
      nonCanonicalAliases.length === 0
    ) {

      canonicalOwner.set(
        key,
        Number(row.id)
      );

    }

  }


  for (const row of rows) {

    const key =
      normText(
        row.canonical_name
      );


    if (!canonicalOwner.has(key)) {

      canonicalOwner.set(
        key,
        Number(row.id)
      );

    }

  }


  const matches =
    new Map();


  for (const row of rows) {

    const canonicalKey =
      normText(
        row.canonical_name
      );


    const candidates = [];


    if (
      canonicalOwner.get(canonicalKey) ===
      Number(row.id)
    ) {

      candidates.push(
        row.canonical_name
      );

    }


    candidates.push(
      row.alias1,
      row.alias2,
      row.alias3,
      row.alias4,
      row.alias5
    );


    for (const value of candidates) {

      const key =
        normText(value);

      if (!key) {
        continue;
      }


      matches.set(
        key,
        {
          match_key:
            key,
          normalization_id:
            Number(row.id),
          canonical_name:
            row.canonical_name,
          k_level:
            row.k_level ||
            null,
          segment:
            row.segment ||
            null,
          updated_at:
            row.updated_at ||
            new Date().toISOString()
        }
      );

    }

  }


  await client.query(
    "DELETE FROM super_dupaza.seller_report_match"
  );


  for (
    const item
    of matches.values()
  ) {

    await client.query(
`INSERT INTO super_dupaza.seller_report_match (
  match_key,
  normalization_id,
  canonical_name,
  k_level,
  segment,
  updated_at
)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT(match_key)
DO UPDATE SET
  normalization_id = EXCLUDED.normalization_id,
  canonical_name = EXCLUDED.canonical_name,
  k_level = EXCLUDED.k_level,
  segment = EXCLUDED.segment,
  updated_at = EXCLUDED.updated_at`,
      [
        item.match_key,
        item.normalization_id,
        item.canonical_name,
        item.k_level,
        item.segment,
        item.updated_at
      ]
    );

  }


  return {
    count:
      matches.size
  };

}


async function pgPendingNameRuleSavePrimary(
  env,
  body
) {

  const id =
    Number(
      body?.id ||
      0
    );


  const canonical =
    String(
      body?.canonical ??
      ""
    ).trim();


  const aliases =
    (
      Array.isArray(body?.aliases)
        ? body.aliases
        : []
    )
      .map(
        value =>
          String(value ?? "")
            .trim()
      )
      .filter(Boolean)
      .filter(
        (value, index, values) =>
          values.findIndex(
            candidate =>
              normalizeMatchText(candidate) ===
              normalizeMatchText(value)
          ) === index
      )
      .filter(
        value =>
          normalizeMatchText(value) !==
          normalizeMatchText(canonical)
      )
      .slice(0, 5);


  if (!canonical) {

    throw new Error(
      "Escribe el nombre final de la normalización."
    );

  }


  const padded =
    [
      ...aliases,
      "",
      "",
      "",
      "",
      ""
    ].slice(0, 5);


  const kLevel =
    validReportK(
      body?.kLevel
    );


  const segment =
    validReportSegment(
      body?.segment
    );


  const now =
    new Date().toISOString();


  return withPg(
    env,
    async client => {

      await client.query("BEGIN");


      try {

        let savedId =
          id;


        if (id > 0) {

          const existing =
            await client.query(
`SELECT id
FROM super_dupaza.pending_name_normalization
WHERE id = $1`,
              [id]
            );


          if (!existing.rows.length) {

            throw new Error(
              "La normalización ya no existe. Recarga la tabla."
            );

          }


          await client.query(
`UPDATE super_dupaza.pending_name_normalization
SET
  canonical_name = $2,
  alias1 = $3,
  alias2 = $4,
  alias3 = $5,
  alias4 = $6,
  alias5 = $7,
  updated_at = $8
WHERE id = $1`,
            [
              id,
              canonical,
              padded[0] || null,
              padded[1] || null,
              padded[2] || null,
              padded[3] || null,
              padded[4] || null,
              now
            ]
          );

        } else {

          const inserted =
            await client.query(
`INSERT INTO super_dupaza.pending_name_normalization (
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5,
  created_at,
  updated_at
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$7)
RETURNING id`,
              [
                canonical,
                padded[0] || null,
                padded[1] || null,
                padded[2] || null,
                padded[3] || null,
                padded[4] || null,
                now
              ]
            );

          savedId =
            Number(
              inserted.rows[0]?.id ||
              0
            );

        }


        if (!kLevel && !segment) {

          await client.query(
`DELETE FROM super_dupaza.seller_report_rule_rows
WHERE normalization_id = $1`,
            [savedId]
          );

        } else {

          await client.query(
`INSERT INTO super_dupaza.seller_report_rule_rows (
  normalization_id,
  k_level,
  segment,
  updated_at
)
VALUES ($1,$2,$3,$4)
ON CONFLICT(normalization_id)
DO UPDATE SET
  k_level = EXCLUDED.k_level,
  segment = EXCLUDED.segment,
  updated_at = EXCLUDED.updated_at`,
            [
              savedId,
              kLevel,
              segment,
              now
            ]
          );

        }


        await pgRebuildSellerReportMatch(
          client
        );


        await client.query(
`INSERT INTO super_dupaza.sync_state (
  source,
  last_success_at,
  records_changed,
  metadata_json
)
VALUES (
  'NAME_NORMALIZATION',
  $1,
  1,
  $2
)
ON CONFLICT(source)
DO UPDATE SET
  last_success_at = EXCLUDED.last_success_at,
  records_changed = super_dupaza.sync_state.records_changed + 1,
  metadata_json = EXCLUDED.metadata_json`,
          [
            now,
            JSON.stringify({
              lastAction:
                "SAVE",
              maxAliasesPerRow:
                5
            })
          ]
        );


        await client.query("COMMIT");


        return {
          ok: true,
          id:
            savedId,
          canonical,
          aliases,
          kLevel,
          segment,
          message:
            "Normalización y clasificación guardadas.",
          databasePrimary:
            "POSTGRESQL"
        };

      } catch (error) {

        await client.query("ROLLBACK");
        throw error;

      }

    }
  );

}


async function mirrorPendingNameRuleToD1(
  env,
  saved
) {

  if (!env.DB) {
    return false;
  }


  const now =
    new Date().toISOString();


  const padded =
    [
      ...(saved.aliases || []),
      "",
      "",
      "",
      "",
      ""
    ].slice(0, 5);


  await env.DB
    .prepare(
`INSERT INTO pending_name_normalization (
  id,
  canonical_name,
  alias1,
  alias2,
  alias3,
  alias4,
  alias5,
  created_at,
  updated_at
)
VALUES (
  ?1,?2,?3,?4,?5,?6,?7,?8,?8
)
ON CONFLICT(id)
DO UPDATE SET
  canonical_name = excluded.canonical_name,
  alias1 = excluded.alias1,
  alias2 = excluded.alias2,
  alias3 = excluded.alias3,
  alias4 = excluded.alias4,
  alias5 = excluded.alias5,
  updated_at = excluded.updated_at`
    )
    .bind(
      saved.id,
      saved.canonical,
      padded[0] || null,
      padded[1] || null,
      padded[2] || null,
      padded[3] || null,
      padded[4] || null,
      now
    )
    .run();


  await saveSellerReportRuleForNormalization(
    env,
    saved.id,
    saved.kLevel,
    saved.segment
  );


  await env.DB
    .prepare(
`UPDATE sync_state
SET
  last_success_at = ?1,
  records_changed = records_changed + 1,
  metadata_json = ?2
WHERE source = 'NAME_NORMALIZATION'`
    )
    .bind(
      now,
      JSON.stringify({
        lastAction:
          "SAVE",
        maxAliasesPerRow:
          5
      })
    )
    .run();


  return true;

}


async function dualPendingNameRuleSave(
  env,
  body
) {

  const primary =
    await pgPendingNameRuleSavePrimary(
      env,
      body
    );


  try {

    await mirrorPendingNameRuleToD1(
      env,
      primary
    );

    return {
      ...primary,
      mirroredToD1:
        true
    };

  } catch (error) {

    console.error(
      "D1_MIRROR_NAME_RULE_SAVE_FAILED",
      error?.message ||
      String(error)
    );

    return {
      ...primary,
      mirroredToD1:
        false,
      mirrorWarning:
        error?.message ||
        String(error)
    };

  }

}


async function pgPendingNameRuleDeletePrimary(
  env,
  idValue
) {

  const id =
    Number(
      idValue ||
      0
    );


  if (
    !Number.isInteger(id) ||
    id <= 0
  ) {

    throw new Error(
      "ID de normalización inválido."
    );

  }


  const now =
    new Date().toISOString();


  return withPg(
    env,
    async client => {

      await client.query("BEGIN");


      try {

        await client.query(
`DELETE FROM super_dupaza.seller_report_match
WHERE normalization_id = $1`,
          [id]
        );


        await client.query(
`DELETE FROM super_dupaza.seller_report_rule_rows
WHERE normalization_id = $1`,
          [id]
        );


        const deleted =
          await client.query(
`DELETE FROM super_dupaza.pending_name_normalization
WHERE id = $1
RETURNING id`,
            [id]
          );


        await pgRebuildSellerReportMatch(
          client
        );


        await client.query(
`INSERT INTO super_dupaza.sync_state (
  source,
  last_success_at,
  records_changed,
  metadata_json
)
VALUES (
  'NAME_NORMALIZATION',
  $1,
  1,
  $2
)
ON CONFLICT(source)
DO UPDATE SET
  last_success_at = EXCLUDED.last_success_at,
  records_changed = super_dupaza.sync_state.records_changed + 1,
  metadata_json = EXCLUDED.metadata_json`,
          [
            now,
            JSON.stringify({
              lastAction:
                "DELETE",
              maxAliasesPerRow:
                5
            })
          ]
        );


        await client.query("COMMIT");


        return {
          ok: true,
          id,
          deleted:
            deleted.rows.length > 0,
          message:
            "Normalización eliminada.",
          databasePrimary:
            "POSTGRESQL"
        };

      } catch (error) {

        await client.query("ROLLBACK");
        throw error;

      }

    }
  );

}


async function dualPendingNameRuleDelete(
  env,
  idValue
) {

  const primary =
    await pgPendingNameRuleDeletePrimary(
      env,
      idValue
    );


  try {

    await pendingNameRuleDelete(
      env,
      idValue
    );

    return {
      ...primary,
      mirroredToD1:
        true
    };

  } catch (error) {

    console.error(
      "D1_MIRROR_NAME_RULE_DELETE_FAILED",
      error?.message ||
      String(error)
    );

    return {
      ...primary,
      mirroredToD1:
        false,
      mirrorWarning:
        error?.message ||
        String(error)
    };

  }

}


// ============================================================
// POSTGRES SHADOW READS
// ============================================================

async function withPg(
  env,
  callback
) {

  if (
    !env.HYPERDRIVE ||
    !env.HYPERDRIVE.connectionString
  ) {

    throw new Error(
      "HYPERDRIVE no está disponible."
    );

  }


  const client =
    new Client({
      connectionString:
        env.HYPERDRIVE.connectionString
    });


  try {

    await client.connect();

    return await callback(
      client
    );

  } finally {

    try {

      await client.end();

    } catch {}

  }

}



function pgBuildReportFilter(
  params,
  groupColumn =
    "group_name",
  segmentColumn =
    "order_type"
) {

  const {
    from,
    to
  } =
    d1RequireDateRange(
      params
    );


  const values = [
    from,
    to
  ];


  const conditions = [
    "report_date BETWEEN $1 AND $2"
  ];


  const filters = [
    [
      "group",
      groupColumn
    ],
    [
      "advisor",
      "advisor"
    ],
    [
      "type",
      segmentColumn
    ],
    [
      "source",
      "source"
    ]
  ];


  for (
    const [
      param,
      column
    ]
    of filters
  ) {

    const value =
      String(
        params.get(
          param
        ) ||
        ""
      )
        .trim();


    if (!value) {
      continue;
    }


    values.push(
      value
    );


    conditions.push(
      `${column} = $${values.length}`
    );

  }


  return {
    from,
    to,
    where:
      conditions.join(
        "\nAND "
      ),
    values
  };

}



async function pgGetReportGroupLabels(
  client
) {

  const result =
    await client.query(
`SELECT
  level,
  display_name,
  sort_order
FROM super_dupaza.report_group_labels
ORDER BY sort_order, level`
    );


  const labels = {
    K1: "K1",
    K2: "K2",
    K3: "K3"
  };


  for (
    const row
    of result.rows
  ) {

    if (
      labels[
        row.level
      ] !==
      undefined
    ) {

      labels[
        row.level
      ] =
        String(
          row.display_name ||
          row.level
        );

    }

  }


  return {
    labels,
    rows:
      result.rows
  };

}



async function pgPendingNameRules(
  client
) {

  const result =
    await client.query(
`SELECT
  n.id,
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  c.k_level,
  c.segment

FROM super_dupaza.pending_name_normalization AS n

LEFT JOIN super_dupaza.seller_report_rule_rows AS c
  ON c.normalization_id =
     n.id

ORDER BY
  LOWER(n.canonical_name),
  n.id`
    );


  return result.rows
    .map(
      pendingNameRowToRule
    );

}



async function pgSellerTargetsForRange(
  client,
  from,
  to
) {

  const months =
    rangeMonthKeys(
      from,
      to
    );


  if (
    !months.length
  ) {

    return {
      targets:
        new Map(),
      sellers:
        new Map()
    };

  }


  const placeholders =
    months
      .map(
        (_, index) =>
          "$" +
          (
            index + 1
          )
      )
      .join(",");


  const result =
    await client.query(
`SELECT
  seller_key,
  seller_name,
  month,
  target_amount
FROM super_dupaza.seller_monthly_targets
WHERE month IN (${placeholders})`,
      months
    );


  const map =
    new Map();

  const sellers =
    new Map();


  for (
    const row
    of result.rows
  ) {

    if (
      !map.has(
        row.seller_key
      )
    ) {

      map.set(
        row.seller_key,
        new Map()
      );

    }


    map.get(
      row.seller_key
    )
      .set(
        row.month,
        numberValue(
          row.target_amount
        )
      );


    if (
      !sellers.has(
        row.seller_key
      )
    ) {

      sellers.set(
        row.seller_key,
        row.seller_name ||
        row.seller_key
      );

    }

  }


  return {
    targets:
      map,
    sellers
  };

}



async function pgSummaryTargetsByLevel(
  client,
  params,
  from,
  to
) {

  const targetInfo =
    await pgSellerTargetsForRange(
      client,
      from,
      to
    );


  const matchResult =
    await client.query(
`SELECT
  match_key,
  k_level,
  segment
FROM super_dupaza.seller_report_match`
    );


  const classification =
    new Map(
      matchResult.rows
        .map(
          row => [
            row.match_key,
            {
              level:
                row.k_level ||
                "K3",
              segment:
                row.segment ||
                "DIGITAL"
            }
          ]
        )
    );


  const selectedGroup =
    String(
      params.get(
        "group"
      ) ||
      ""
    )
      .trim();


  const selectedAdvisor =
    normalizeMatchText(
      params.get(
        "advisor"
      ) ||
      ""
    );


  const selectedSegment =
    String(
      params.get(
        "type"
      ) ||
      ""
    )
      .trim();


  const totals = {
    K1: 0,
    K2: 0,
    K3: 0
  };


  for (
    const [
      sellerKey,
      sellerName
    ]
    of targetInfo.sellers.entries()
  ) {

    if (
      selectedAdvisor &&
      selectedAdvisor !==
        sellerKey
    ) {
      continue;
    }


    const rule =
      classification.get(
        sellerKey
      ) ||
      {
        level:
          "K3",
        segment:
          "DIGITAL"
      };


    const level =
      [
        "K1",
        "K2",
        "K3"
      ]
        .includes(
          rule.level
        )
          ? rule.level
          : "K3";


    const segment =
      [
        "DIGITAL",
        "PETAPA",
        "XENACOJ"
      ]
        .includes(
          rule.segment
        )
          ? rule.segment
          : "DIGITAL";


    if (
      selectedGroup &&
      selectedGroup !==
        level
    ) {
      continue;
    }


    if (
      selectedSegment &&
      selectedSegment !==
        segment
    ) {
      continue;
    }


    totals[
      level
    ] +=
      proratedTargetForRange(
        sellerName,
        from,
        to,
        targetInfo.targets
      );

  }


  return {
    K1:
      round2(
        totals.K1
      ),
    K2:
      round2(
        totals.K2
      ),
    K3:
      round2(
        totals.K3
      )
  };

}



async function pgPublicStatus(
  env
) {

  return withPg(
    env,
    async client => {

      const syncResult =
        await client.query(
`SELECT
  source,
  last_started_at,
  last_success_at,
  range_from,
  range_to,
  records_seen,
  records_changed,
  metadata_json,
  last_error
FROM super_dupaza.sync_state
ORDER BY source`
        );


      const orderResult =
        await client.query(
          "SELECT COUNT(*)::bigint AS n FROM super_dupaza.zas_orders"
        );


      const states =
        syncResult.rows;


      const bySource =
        new Map(
          states.map(
            row => [
              row.source,
              row
            ]
          )
        );


      const bo =
        bySource.get(
          "BO_VT"
        ) ||
        {};


      const zas =
        bySource.get(
          "ZASCONNECT"
        ) ||
        {};


      const reports =
        bySource.get(
          "REPORTS"
        ) ||
        {};


      const scheduler =
        bySource.get(
          "SCHEDULER"
        ) ||
        {};


      let schedulerMeta = {};
      let boMeta = {};


      try {

        schedulerMeta =
          JSON.parse(
            scheduler.metadata_json ||
            "{}"
          );

      } catch {}


      try {

        boMeta =
          JSON.parse(
            bo.metadata_json ||
            "{}"
          );

      } catch {}


      const lastSyncAt =
        [
          bo.last_success_at,
          zas.last_success_at,
          reports.last_success_at
        ]
          .filter(Boolean)
          .sort()
          .at(-1) ||
        null;


      const ageMinutes =
        lastSyncAt
          ? Math.max(
              0,
              Math.round(
                (
                  Date.now() -
                  new Date(
                    lastSyncAt
                  ).getTime()
                ) /
                60000
              )
            )
          : null;


      const schedulerLastSuccess =
        scheduler.last_success_at ||
        null;


      const schedulerAgeMinutes =
        schedulerLastSuccess
          ? Math.max(
              0,
              Math.round(
                (
                  Date.now() -
                  new Date(
                    schedulerLastSuccess
                  ).getTime()
                ) /
                60000
              )
            )
          : null;


      return {
        ok: true,

        source:
          "POSTGRESQL",

        googleSheets: {
          ok:
            !bo.last_error,

          lastImport:
            boMeta.lastImport ||
            bo.last_success_at ||
            null,

          sourceModified:
            boMeta.sourceModified ||
            null,

          lastSyncAt:
            bo.last_success_at ||
            null
        },

        zasConnect: {
          ok:
            !zas.last_error,

          orders:
            Number(
              orderResult.rows[0]?.n ||
              0
            ),

          lastSyncAt:
            zas.last_success_at ||
            null,

          recordsChanged:
            Number(
              zas.records_changed ||
              0
            )
        },

        generatedAt:
          lastSyncAt,

        lastSyncAt,

        ageMinutes,

        stale:
          ageMinutes === null ||
          ageMinutes > 12,

        scheduler: {
          hasRun:
            Boolean(
              scheduler.last_started_at ||
              scheduler.last_success_at
            ),

          lastStartedAt:
            scheduler.last_started_at ||
            scheduler.last_success_at ||
            null,

          lastSuccessAt:
            schedulerLastSuccess,

          cron:
            schedulerMeta.cron ||
            null,

          days:
            Number(
              schedulerMeta.days ||
              0
            ),

          event:
            schedulerMeta.event ||
            null,

          lastError:
            scheduler.last_error ||
            null,

          ageMinutes:
            schedulerAgeMinutes,

          stale:
            !schedulerLastSuccess ||
            schedulerAgeMinutes > 12
        },

        d1: {
          rangeFrom:
            reports.range_from ||
            null,

          rangeTo:
            reports.range_to ||
            null
        },

        postgres: {
          rangeFrom:
            reports.range_from ||
            null,

          rangeTo:
            reports.range_to ||
            null
        },

        cacheSeconds:
          0
      };

    }
  );

}



async function pgNotLoadedDashboard(
  env
) {

  return withPg(
    env,
    async client => {

      const ordersResult =
        await client.query(
`SELECT
  z.*

FROM super_dupaza.zas_orders AS z

LEFT JOIN super_dupaza.loaded_refs AS r
  ON r.tracking_code =
     z.tracking_code

LEFT JOIN super_dupaza.manual_sap_control AS m
  ON m.tracking_code =
     z.tracking_code

WHERE r.tracking_code IS NULL
  AND m.tracking_code IS NULL

ORDER BY
  z.order_date DESC,
  z.tracking_code`
        );


      const linesResult =
        await client.query(
`SELECT
  l.tracking_code,
  l.line_key,
  l.line_number,
  l.sku AS line_sku,
  l.description AS line_description,
  l.quantity AS line_quantity,
  l.brand AS line_brand,
  l.location_code AS line_location_code,
  l.ring_code AS line_ring_code,
  l.unit_price AS line_unit_price,
  l.line_total AS line_total

FROM super_dupaza.zas_order_lines AS l

JOIN super_dupaza.zas_orders AS z
  ON z.tracking_code =
     l.tracking_code

LEFT JOIN super_dupaza.loaded_refs AS r
  ON r.tracking_code =
     z.tracking_code

LEFT JOIN super_dupaza.manual_sap_control AS m
  ON m.tracking_code =
     z.tracking_code

WHERE r.tracking_code IS NULL
  AND m.tracking_code IS NULL

ORDER BY
  z.order_date DESC,
  l.tracking_code,
  CASE
    WHEN l.line_number IS NULL
      THEN 1
    ELSE 0
  END,
  l.line_number,
  l.line_key`
        );


      const orderRows =
        ordersResult.rows;


      const lineRows =
        linesResult.rows;


      const linesByTracking =
        new Map();


      for (
        const line
        of lineRows
      ) {

        const tracking =
          normId(
            line.tracking_code
          );


        if (!tracking) {
          continue;
        }


        if (
          !linesByTracking.has(
            tracking
          )
        ) {

          linesByTracking.set(
            tracking,
            []
          );

        }


        linesByTracking
          .get(
            tracking
          )
          .push(
            line
          );

      }


      const apiRows = [];


      for (
        const order
        of orderRows
      ) {

        const tracking =
          normId(
            order.tracking_code
          );


        const lines =
          linesByTracking.get(
            tracking
          ) ||
          [];


        if (!lines.length) {

          apiRows.push(
            d1PendingJoinedToApi(
              {
                ...order,
                line_key: null,
                line_number: null,
                line_sku: null,
                line_description: null,
                line_quantity: null,
                line_brand: null,
                line_location_code: null,
                line_ring_code: null,
                line_unit_price: null,
                line_total: null
              }
            )
          );


          continue;

        }


        for (
          const line
          of lines
        ) {

          apiRows.push(
            d1PendingJoinedToApi(
              {
                ...order,
                ...line
              }
            )
          );

        }

      }


      const nameRules =
        await pgPendingNameRules(
          client
        );


      const built =
        buildNotLoaded(
          apiRows,
          [],
          nameRules
        );


      return {
        ok: true,

        source:
          "POSTGRESQL",

        rows:
          built.rows,

        columns:
          NOT_LOADED_COLUMNS,

        kpis:
          built.kpis,

        options:
          buildNotLoadedOptions(
            built.rows
          ),

        diagnostics: {
          pendingOrderHeaders:
            orderRows.length,

          pendingProductLines:
            lineRows.length,

          renderedRows:
            built.rows.length
        },

        updatedAt:
          new Date()
            .toISOString()
      };

    }
  );

}



async function pgReportConfigGet(
  env
) {

  return withPg(
    env,
    async client => {

      const info =
        await pgGetReportGroupLabels(
          client
        );


      return {
        ok: true,

        source:
          "POSTGRESQL",

        labels:
          info.labels,

        note:
          "Los nombres visuales no modifican el código interno K1/K2/K3. K y segmento por vendedor se configuran en ⚙ NOMBRES."
      };

    }
  );

}



async function pgPendingNameRulesList(
  env
) {

  return withPg(
    env,
    async client => {

      const rulesResult =
        await client.query(
`SELECT
  n.id,
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  n.updated_at,
  c.k_level,
  c.segment

FROM super_dupaza.pending_name_normalization AS n

LEFT JOIN super_dupaza.seller_report_rule_rows AS c
  ON c.normalization_id =
     n.id

ORDER BY
  LOWER(n.canonical_name),
  n.id`
        );


      const sellersResult =
        await client.query(
`SELECT
  seller_name
FROM super_dupaza.zas_orders
WHERE seller_name IS NOT NULL
  AND TRIM(seller_name) <> ''
GROUP BY seller_name
ORDER BY LOWER(seller_name)`
        );


      const rules =
        rulesResult.rows
          .map(
            row => ({
              ...pendingNameRowToRule(
                row
              ),
              updatedAt:
                row.updated_at ||
                null
            })
          );


      return {
        ok: true,

        source:
          "POSTGRESQL",

        rules,

        rawSellers:
          sellersResult.rows
            .map(
              row =>
                row.seller_name
            )
            .filter(Boolean),

        maxAliases:
          5,

        kLevels: [
          "K1",
          "K2",
          "K3"
        ],

        segments: [
          "DIGITAL",
          "PETAPA",
          "XENACOJ"
        ],

        reportLogic: {
          apiDefault:
            "DIGITAL",
          pdvBoVt:
            "TIENDA_FISICA",
          physicalSegments: [
            "PETAPA",
            "XENACOJ"
          ]
        }
      };

    }
  );

}



async function pgSellerTargetsList(
  env,
  params
) {

  return withPg(
    env,
    async client => {

      const month =
        validTargetMonth(
          params.get(
            "month"
          ) ||
          guatemalaMonthKey()
        );


      const sellersResult =
        await client.query(
`SELECT seller_name, priority
FROM (
  SELECT advisor AS seller_name, 1 AS priority
  FROM super_dupaza.report_orders
  WHERE advisor IS NOT NULL
    AND TRIM(advisor) <> ''

  UNION ALL

  SELECT seller_name, 2 AS priority
  FROM super_dupaza.zas_orders
  WHERE seller_name IS NOT NULL
    AND TRIM(seller_name) <> ''

  UNION ALL

  SELECT canonical_name AS seller_name, 3 AS priority
  FROM super_dupaza.pending_name_normalization
  WHERE canonical_name IS NOT NULL
    AND TRIM(canonical_name) <> ''
) AS sellers
ORDER BY priority, LOWER(seller_name)`
        );


      const targetResult =
        await client.query(
`SELECT
  seller_key,
  seller_name,
  target_amount
FROM super_dupaza.seller_monthly_targets
WHERE month = $1
ORDER BY LOWER(seller_name)`,
          [
            month
          ]
        );


      const targetMap =
        new Map(
          targetResult.rows
            .map(
              row => [
                row.seller_key,
                row
              ]
            )
        );


      const sellers =
        new Map();


      for (
        const row
        of sellersResult.rows
      ) {

        const name =
          String(
            row.seller_name ||
            ""
          )
            .trim();


        const key =
          normalizeMatchText(
            name
          );


        if (
          !name ||
          !key ||
          sellers.has(
            key
          )
        ) {
          continue;
        }


        sellers.set(
          key,
          name
        );

      }


      for (
        const row
        of targetResult.rows
      ) {

        if (
          !sellers.has(
            row.seller_key
          )
        ) {

          sellers.set(
            row.seller_key,
            row.seller_name
          );

        }

      }


      const rows =
        [
          ...sellers.entries()
        ]
          .map(
            ([
              sellerKey,
              seller
            ]) => ({
              sellerKey,
              seller,
              target:
                round2(
                  numberValue(
                    targetMap.get(
                      sellerKey
                    )?.target_amount
                  )
                )
            })
          )
          .sort(
            (a, b) =>
              a.seller.localeCompare(
                b.seller,
                "es"
              )
          );


      return {
        ok: true,
        source:
          "POSTGRESQL",
        month,
        rows
      };

    }
  );

}



async function pgManualApiImportRowsPrimary(
  env,
  body
) {

  return withPg(
    env,
    async client => {

      await client.query(
        "BEGIN"
      );


      try {

        const pgEnv = {
          ...env,

          DB:
            makePgD1Compat(
              client
            )
        };


        const result =
          await manualApiImportRows(
            pgEnv,
            body
          );


        await client.query(
          "COMMIT"
        );


        return {
          ...result,

          engine:
            "POSTGRESQL",

          via:
            "HYPERDRIVE",

          transactionCommitted:
            true,

          d1Modified:
            false
        };

      } catch (error) {

        try {
          await client.query(
            "ROLLBACK"
          );
        } catch {}


        throw error;

      }

    }
  );

}


async function pgManualApiImportList(
  env
) {

  return withPg(
    env,
    async client => {

      const imports =
        await client.query(
`SELECT
  import_id,
  file_name,
  imported_at,
  source_rows,
  unique_orders,
  imported_orders,
  updated_manual_orders,
  skipped_existing_api,
  imported_lines,
  total_amount,
  report_rows_changed
FROM super_dupaza.manual_zas_api_imports
ORDER BY imported_at DESC
LIMIT 20`
        );


      const registry =
        await client.query(
`SELECT
  COUNT(*)::bigint AS orders,
  COALESCE(SUM(row_count), 0) AS lines,
  COALESCE(SUM(order_total), 0) AS total,
  SUM(
    CASE
      WHEN api_seen_at IS NOT NULL
      THEN 1
      ELSE 0
    END
  ) AS api_seen
FROM super_dupaza.manual_zas_api_registry`
        );


      const current =
        registry.rows[0] ||
        {};


      return {
        ok: true,
        source:
          "POSTGRESQL",

        current: {
          protectedOrders:
            Number(
              current.orders ||
              0
            ),

          productLines:
            Number(
              current.lines ||
              0
            ),

          totalAmount:
            round2(
              numberValue(
                current.total
              )
            ),

          nowAlsoSeenInApi:
            Number(
              current.api_seen ||
              0
            )
        },

        imports:
          imports.rows
      };

    }
  );

}



async function pgManualSapList(
  env
) {

  return withPg(
    env,
    async client => {

      const result =
        await client.query(
`SELECT
  tracking_code,
  status,
  marked_sap_at,
  resolved_at,
  updated_at,
  customer_name,
  seller_name,
  order_date,
  order_total
FROM super_dupaza.manual_sap_control
ORDER BY
  CASE status
    WHEN 'EN_SAP' THEN 1
    WHEN 'RESUELTO' THEN 2
    ELSE 9
  END,
  updated_at DESC`
        );


      const rows =
        result.rows;


      return {
        ok: true,

        source:
          "POSTGRESQL",

        active:
          rows.filter(
            row =>
              row.status ===
              "EN_SAP"
          ),

        resolved:
          rows.filter(
            row =>
              row.status ===
              "RESUELTO"
          ),

        counts: {
          active:
            rows.filter(
              row =>
                row.status ===
                "EN_SAP"
            )
              .length,

          resolved:
            rows.filter(
              row =>
                row.status ===
                "RESUELTO"
            )
              .length
        }
      };

    }
  );

}



async function pgReportOptions(
  env
) {

  return withPg(
    env,
    async client => {

      const result =
        await client.query(
`SELECT
  advisor,
  source,
  report_date
FROM super_dupaza.report_orders`
        );


      const labelInfo =
        await pgGetReportGroupLabels(
          client
        );


      const rows =
        result.rows;


      const unique =
        values =>
          [
            ...new Set(
              values
                .filter(Boolean)
            )
          ]
            .sort(
              (
                a,
                b
              ) =>
                String(a)
                  .localeCompare(
                    String(b),
                    "es"
                  )
            );


      const dates =
        rows
          .map(
            row =>
              row.report_date
          )
          .filter(Boolean)
          .sort();


      return {
        ok: true,

        source:
          "POSTGRESQL",

        groupLabels:
          labelInfo.labels,

        options: {
          groups:
            [
              "K1",
              "K2",
              "K3"
            ]
              .map(
                level => ({
                  value:
                    level,

                  label:
                    labelInfo.labels[
                      level
                    ] ||
                    level
                })
              ),

          advisors:
            unique(
              rows.map(
                row =>
                  row.advisor
              )
            ),

          types: [
            "DIGITAL",
            "PETAPA",
            "XENACOJ"
          ],

          sources:
            unique(
              rows.map(
                row =>
                  row.source
              )
            ),

          minDate:
            dates[0] ||
            null,

          maxDate:
            dates.at(
              -1
            ) ||
            null
        }
      };

    }
  );

}



async function pgSummary(
  env,
  params
) {

  return withPg(
    env,
    async client => {

      const labelInfo =
        await pgGetReportGroupLabels(
          client
        );


      const filter =
        pgBuildReportFilter(
          params,
          "effective_level",
          "effective_segment"
        );


      const effectiveCte =
        sellerReportEffectiveCteSql()
          .replace(
            /FROM report_orders AS r/g,
            "FROM super_dupaza.report_orders AS r"
          )
          .replace(
            /LEFT JOIN loaded_refs AS lr/g,
            "LEFT JOIN super_dupaza.loaded_refs AS lr"
          )
          .replace(
            /LEFT JOIN seller_report_match AS srm/g,
            "LEFT JOIN super_dupaza.seller_report_match AS srm"
          );


      const matrixSql =
effectiveCte +
`SELECT
  effective_level AS level,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'DIGITAL'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS digital,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'PETAPA'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS petapa,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'XENACOJ'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS xenacoj,

  ROUND(
    SUM(amount)::numeric,
    2
  ) AS total

FROM effective

WHERE ${filter.where}

GROUP BY effective_level

ORDER BY
  CASE effective_level
    WHEN 'K1' THEN 1
    WHEN 'K2' THEN 2
    WHEN 'K3' THEN 3
    ELSE 99
  END`;


      const chartSql =
effectiveCte +
`SELECT
  report_date AS date,

  ROUND(
    SUM(
      CASE
        WHEN effective_level = 'K1'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS amount_k1,

  ROUND(
    SUM(
      CASE
        WHEN effective_level = 'K2'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS amount_k2,

  ROUND(
    SUM(
      CASE
        WHEN effective_level = 'K3'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS amount_k3,

  ROUND(
    SUM(amount)::numeric,
    2
  ) AS total

FROM effective

WHERE ${filter.where}

GROUP BY report_date

ORDER BY report_date`;


      const kpiSql =
effectiveCte +
`SELECT
  COUNT(*)::bigint AS orders,

  ROUND(
    SUM(
      CASE
        WHEN source = 'ZASCONNECT'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS zas_connect,

  ROUND(
    SUM(
      CASE
        WHEN source = 'PDV BO/VT'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS pdv_added,

  ROUND(
    SUM(amount)::numeric,
    2
  ) AS total

FROM effective

WHERE ${filter.where}`;


      const matrixResult =
        await client.query(
          matrixSql,
          filter.values
        );


      const chartResult =
        await client.query(
          chartSql,
          filter.values
        );


      const kpiResult =
        await client.query(
          kpiSql,
          filter.values
        );


      const byLevel =
        new Map(
          matrixResult.rows
            .map(
              row => [
                row.level,
                row
              ]
            )
        );


      const targetsByLevel =
        await pgSummaryTargetsByLevel(
          client,
          params,
          filter.from,
          filter.to
        );


      const rows =
        [
          "K1",
          "K2",
          "K3"
        ]
          .map(
            level => {

              const source =
                byLevel.get(
                  level
                ) ||
                {};


              return {
                level,

                grupo:
                  labelInfo.labels[
                    level
                  ] ||
                  level,

                DIGITAL:
                  round2(
                    numberValue(
                      source.digital
                    )
                  ),

                PETAPA:
                  round2(
                    numberValue(
                      source.petapa
                    )
                  ),

                XENACOJ:
                  round2(
                    numberValue(
                      source.xenacoj
                    )
                  ),

                TOTAL:
                  round2(
                    numberValue(
                      source.total
                    )
                  ),

                META:
                  round2(
                    targetsByLevel[
                      level
                    ] ||
                    0
                  ),

                TENDENCIA:
                  trendForCurrentMonth(
                    numberValue(
                      source.total
                    ),
                    filter.from,
                    filter.to
                  )
              };

            }
          );


      rows.push({
        level:
          "TOTAL",

        grupo:
          "RESUMEN",

        DIGITAL:
          round2(
            sum(
              rows.map(
                row =>
                  row.DIGITAL
              )
            )
          ),

        PETAPA:
          round2(
            sum(
              rows.map(
                row =>
                  row.PETAPA
              )
            )
          ),

        XENACOJ:
          round2(
            sum(
              rows.map(
                row =>
                  row.XENACOJ
              )
            )
          ),

        TOTAL:
          round2(
            sum(
              rows.map(
                row =>
                  row.TOTAL
              )
            )
          ),

        META:
          round2(
            sum(
              rows.map(
                row =>
                  row.META
              )
            )
          ),

        TENDENCIA:
          trendForCurrentMonth(
            sum(
              rows.map(
                row =>
                  row.TOTAL
              )
            ),
            filter.from,
            filter.to
          )
      });


      const chart =
        chartResult.rows
          .map(
            row => {

              const total =
                numberValue(
                  row.total
                );


              const k1 =
                numberValue(
                  row.amount_k1
                );


              const k2 =
                numberValue(
                  row.amount_k2
                );


              const k3 =
                numberValue(
                  row.amount_k3
                );


              return {
                date:
                  row.date,

                K1:
                  total
                    ? round2(
                        k1 /
                        total *
                        100
                      )
                    : 0,

                K2:
                  total
                    ? round2(
                        k2 /
                        total *
                        100
                      )
                    : 0,

                K3:
                  total
                    ? round2(
                        k3 /
                        total *
                        100
                      )
                    : 0,

                amountK1:
                  round2(
                    k1
                  ),

                amountK2:
                  round2(
                    k2
                  ),

                amountK3:
                  round2(
                    k3
                  ),

                total:
                  round2(
                    total
                  )
              };

            }
          );


      const kpi =
        kpiResult.rows[0] ||
        {};


      return {
        ok: true,

        source:
          "POSTGRESQL",

        range: {
          from:
            filter.from,

          to:
            filter.to
        },

        groupLabels:
          labelInfo.labels,

        rows,

        chart,

        kpis: {
          zasConnect:
            round2(
              numberValue(
                kpi.zas_connect
              )
            ),

          pdvAdded:
            round2(
              numberValue(
                kpi.pdv_added
              )
            ),

          total:
            round2(
              numberValue(
                kpi.total
              )
            ),

          orders:
            Number(
              kpi.orders ||
              0
            ),

          target:
            round2(
              rows[
                rows.length - 1
              ]?.META ||
              0
            ),

          trend:
            round2(
              rows[
                rows.length - 1
              ]?.TENDENCIA ||
              0
            )
        }
      };

    }
  );

}



async function pgAdvisorSummary(
  env,
  params
) {

  return withPg(
    env,
    async client => {

      const {
        from,
        to
      } =
        d1RequireDateRange(
          params
        );


      const effectiveCte =
        sellerReportEffectiveCteSql()
          .replace(
            /FROM report_orders AS r/g,
            "FROM super_dupaza.report_orders AS r"
          )
          .replace(
            /LEFT JOIN loaded_refs AS lr/g,
            "LEFT JOIN super_dupaza.loaded_refs AS lr"
          )
          .replace(
            /LEFT JOIN seller_report_match AS srm/g,
            "LEFT JOIN super_dupaza.seller_report_match AS srm"
          );


      const sql =
effectiveCte +
`SELECT
  advisor AS asesor,

  ROUND(
    SUM(amount)::numeric,
    2
  ) AS monto,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'DIGITAL'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS digital,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'PETAPA'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS petapa,

  ROUND(
    SUM(
      CASE
        WHEN effective_segment = 'XENACOJ'
        THEN amount
        ELSE 0
      END
    )::numeric,
    2
  ) AS xenacoj

FROM effective

WHERE report_date
BETWEEN $1 AND $2

GROUP BY advisor

ORDER BY monto DESC,
         asesor ASC`;


      const result =
        await client.query(
          sql,
          [
            from,
            to
          ]
        );


      const targetInfo =
        await pgSellerTargetsForRange(
          client,
          from,
          to
        );


      const targetMap =
        targetInfo.targets;


      const rows =
        result.rows
          .map(
            row => ({
              ASESOR:
                row.asesor,

              MONTO:
                round2(
                  numberValue(
                    row.monto
                  )
                ),

              DIGITAL:
                round2(
                  numberValue(
                    row.digital
                  )
                ),

              PETAPA:
                round2(
                  numberValue(
                    row.petapa
                  )
                ),

              XENACOJ:
                round2(
                  numberValue(
                    row.xenacoj
                  )
                ),

              META:
                proratedTargetForRange(
                  row.asesor,
                  from,
                  to,
                  targetMap
                ),

              TENDENCIA:
                trendForCurrentMonth(
                  row.monto,
                  from,
                  to
                )
            })
          );


      const existingSellerKeys =
        new Set(
          rows.map(
            row =>
              normalizeMatchText(
                row.ASESOR
              )
          )
        );


      for (
        const [
          sellerKey,
          sellerName
        ]
        of targetInfo.sellers.entries()
      ) {

        if (
          existingSellerKeys.has(
            sellerKey
          )
        ) {
          continue;
        }


        rows.push({
          ASESOR:
            sellerName,
          MONTO:
            0,
          DIGITAL:
            0,
          PETAPA:
            0,
          XENACOJ:
            0,
          META:
            proratedTargetForRange(
              sellerName,
              from,
              to,
              targetMap
            ),
          TENDENCIA:
            0
        });

      }


      rows.sort(
        (a, b) =>
          Number(
            b.MONTO ||
            0
          ) -
          Number(
            a.MONTO ||
            0
          ) ||
          String(
            a.ASESOR ||
            ""
          )
            .localeCompare(
              String(
                b.ASESOR ||
                ""
              ),
              "es"
            )
      );


      const totalRow = {
        ASESOR:
          "TOTAL",

        MONTO:
          round2(
            sum(
              rows.map(
                row =>
                  row.MONTO
              )
            )
          ),

        DIGITAL:
          round2(
            sum(
              rows.map(
                row =>
                  row.DIGITAL
              )
            )
          ),

        PETAPA:
          round2(
            sum(
              rows.map(
                row =>
                  row.PETAPA
              )
            )
          ),

        XENACOJ:
          round2(
            sum(
              rows.map(
                row =>
                  row.XENACOJ
              )
            )
          ),

        META:
          round2(
            sum(
              rows.map(
                row =>
                  row.META
              )
            )
          ),

        TENDENCIA:
          trendForCurrentMonth(
            sum(
              rows.map(
                row =>
                  row.MONTO
              )
            ),
            from,
            to
          )
      };


      rows.push(
        totalRow
      );


      return {
        ok: true,

        source:
          "POSTGRESQL",

        range: {
          from,
          to
        },

        rows,

        kpis: {
          advisors:
            Math.max(
              0,
              rows.length -
              1
            ),

          total:
            totalRow.MONTO,

          digital:
            totalRow.DIGITAL,

          petapa:
            totalRow.PETAPA,

          xenacoj:
            totalRow.XENACOJ,

          target:
            totalRow.META,

          trend:
            totalRow.TENDENCIA
        }
      };

    }
  );

}


// ============================================================
// POSTGRES / HYPERDRIVE - SMOKE TEST
// ============================================================

async function postgresSmokeTest(
  env
) {

  if (
    !env.HYPERDRIVE ||
    !env.HYPERDRIVE.connectionString
  ) {

    return {
      ok: false,
      stage: "binding",
      error: "HYPERDRIVE no está disponible."
    };

  }


  const client =
    new Client({
      connectionString:
        env.HYPERDRIVE.connectionString
    });


  try {

    await client.connect();


    const result =
      await client.query(`
        select
          current_user as current_user,
          current_database() as current_database,
          current_schema() as current_schema,
          current_setting('search_path') as search_path,

          (select count(*)::bigint from super_dupaza.loaded_refs)
            as loaded_refs,

          (select count(*)::bigint from super_dupaza.report_orders)
            as report_orders,

          (select count(*)::bigint from super_dupaza.zas_orders)
            as zas_orders,

          (select count(*)::bigint from super_dupaza.zas_order_lines)
            as zas_order_lines
      `);


    const row =
      result.rows[0] ||
      {};


    const counts = {
      loaded_refs:
        Number(
          row.loaded_refs ||
          0
        ),

      report_orders:
        Number(
          row.report_orders ||
          0
        ),

      zas_orders:
        Number(
          row.zas_orders ||
          0
        ),

      zas_order_lines:
        Number(
          row.zas_order_lines ||
          0
        )
    };


    const verified =
      counts.loaded_refs ===
        7031 &&
      counts.report_orders ===
        2435 &&
      counts.zas_orders ===
        430 &&
      counts.zas_order_lines ===
        1200;


    return {
      ok: true,
      verified,

      connection: {
        current_user:
          row.current_user,

        current_database:
          row.current_database,

        current_schema:
          row.current_schema,

        search_path:
          row.search_path
      },

      counts,

      note:
        "El dashboard sigue usando D1. Esta ruta solo valida pg + Hyperdrive dentro del Worker real."
    };

  } catch (error) {

    return {
      ok: false,
      stage: "postgres",
      code:
        error?.code ||
        null,
      error:
        error?.message ||
        String(error)
    };

  } finally {

    try {

      await client.end();

    } catch {}

  }

}


// ============================================================
// HYPERDRIVE - PRUEBA DE BINDING
// ============================================================

function hyperdriveBindingInfo(
  env
) {

  const binding =
    env.HYPERDRIVE;


  if (!binding) {

    return {
      ok:
        false,

      binding:
        "HYPERDRIVE",

      present:
        false,

      error:
        "No existe env.HYPERDRIVE en este Worker."
    };

  }


  const connectionString =
    String(
      binding.connectionString ||
      ""
    );


  if (!connectionString) {

    return {
      ok:
        false,

      binding:
        "HYPERDRIVE",

      present:
        true,

      hasConnectionString:
        false,

      error:
        "El binding existe pero no expone connectionString."
    };

  }


  let parsed;


  try {

    parsed =
      new URL(
        connectionString
      );

  } catch {

    return {
      ok:
        false,

      binding:
        "HYPERDRIVE",

      present:
        true,

      hasConnectionString:
        true,

      error:
        "Cloudflare entregó connectionString pero no pudo analizarse como URL."
    };

  }


  return {
    ok:
      true,

    binding:
      "HYPERDRIVE",

    present:
      true,

    hasConnectionString:
      true,

    protocol:
      parsed.protocol,

    hostname:
      parsed.hostname,

    port:
      parsed.port ||
      "5432",

    database:
      parsed.pathname
        .replace(
          /^\/+/,
          ""
        ),

    usernamePresent:
      Boolean(
        parsed.username
      ),

    passwordPresent:
      Boolean(
        parsed.password
      ),

    ip:
      binding.ip ||
      null,

    note:
      "La contraseña y la connectionString completa nunca se devuelven."
  };

}


function hyperdriveTestPage() {

  return new Response(
    String.raw`<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta
    name="viewport"
    content="width=device-width,initial-scale=1"
  >
  <title>SUPER DUPAZA · Hyperdrive Test</title>

  <style>
    *{box-sizing:border-box}
    body{
      margin:0;
      background:#f4f6f8;
      color:#101828;
      font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    }
    .wrap{
      max-width:820px;
      margin:40px auto;
      padding:0 18px;
    }
    .card{
      background:#fff;
      border:1px solid #d0d5dd;
      border-radius:16px;
      padding:22px;
      box-shadow:0 8px 28px rgba(16,24,40,.06);
    }
    h1{margin:0 0 8px;font-size:26px}
    p{color:#475467;line-height:1.5}
    label{
      display:block;
      margin:18px 0 6px;
      font-size:13px;
      font-weight:800;
    }
    input{
      width:100%;
      padding:11px 12px;
      border:1px solid #98a2b3;
      border-radius:9px;
      font:inherit;
    }
    button{
      margin-top:14px;
      border:0;
      border-radius:9px;
      padding:11px 16px;
      font-weight:900;
      cursor:pointer;
      background:#111827;
      color:#fff;
    }
    button:disabled{opacity:.45}
    pre{
      margin-top:18px;
      padding:14px;
      border:1px solid #e4e7ec;
      border-radius:10px;
      background:#f9fafb;
      white-space:pre-wrap;
      overflow-wrap:anywhere;
      min-height:150px;
      font-size:12px;
    }
    .note{
      padding:12px 14px;
      border-radius:10px;
      background:#eff8ff;
      border:1px solid #b2ddff;
      color:#175cd3;
      font-weight:700;
    }
  </style>
</head>

<body>
  <main class="wrap">
    <section class="card">

      <h1>Prueba de binding Hyperdrive</h1>

      <p>
        Comprueba que el Worker ve HYPERDRIVE y recibe
        una connectionString interna válida.
      </p>

      <div class="note">
        Esta prueba no ejecuta SQL y no muestra contraseñas.
        D1 continúa siendo el motor activo.
      </div>

      <label for="token">
        D1_SYNC_TOKEN
      </label>

      <input
        id="token"
        type="password"
        autocomplete="off"
        placeholder="Pega tu token administrativo"
      >

      <button id="btn">
        PROBAR HYPERDRIVE
      </button>

      <pre id="out">
Esperando prueba...
      </pre>

    </section>
  </main>


<script>
const token =
  document.getElementById(
    "token"
  );

const btn =
  document.getElementById(
    "btn"
  );

const out =
  document.getElementById(
    "out"
  );


btn.addEventListener(
  "click",
  async () => {

    const value =
      token.value
        .trim();


    if (!value) {

      alert(
        "Escribe D1_SYNC_TOKEN."
      );

      return;

    }


    btn.disabled =
      true;

    out.textContent =
      "Probando binding HYPERDRIVE...";


    try {

      const response =
        await fetch(
          "/api/hyperdrive-binding-test?ts=" +
          Date.now(),
          {
            cache:
              "no-store",

            headers: {
              "X-Super-Dupaza-Admin":
                value
            }
          }
        );


      const data =
        await response.json();


      out.textContent =
        JSON.stringify(
          data,
          null,
          2
        );

    } catch (error) {

      out.textContent =
        "ERROR: " +
        String(
          error?.message ||
          error
        );

    } finally {

      btn.disabled =
        false;

    }

  }
);
</script>

</body>
</html>`,
    {
      headers: {
        "content-type":
          "text/html; charset=utf-8",

        "cache-control":
          "no-store"
      }
    }
  );

}


function d1AdminPage() {

  return new Response(
String.raw`<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SUPER DUPAZA · D1 Admin</title>
<style>
body{
  margin:0;
  font-family:Segoe UI,Arial,sans-serif;
  background:#f4f6f8;
  color:#202124;
}
main{
  max-width:720px;
  margin:50px auto;
  padding:20px;
}
.card{
  background:#fff;
  border:1px solid #e2e6ea;
  border-radius:14px;
  padding:22px;
}
h1{margin-top:0}
label{
  display:block;
  font-weight:700;
  margin-bottom:7px;
}
input{
  width:100%;
  padding:11px;
  border:1px solid #d0d5dd;
  border-radius:8px;
  box-sizing:border-box;
}
button{
  margin-top:12px;
  padding:11px 16px;
  border:0;
  border-radius:8px;
  background:#f4c400;
  font-weight:800;
  cursor:pointer;
}
pre{
  margin-top:18px;
  padding:14px;
  overflow:auto;
  background:#131313;
  color:#eee;
  border-radius:9px;
  min-height:120px;
  white-space:pre-wrap;
}
.note{
  color:#667085;
  font-size:13px;
  line-height:1.45;
}
</style>
</head>
<body>
<main>
<div class="card">
<h1>SUPER DUPAZA · D1</h1>

<p class="note">
Esta pantalla ejecuta la carga inicial manual optimizada para D1 Free.
El token se envía en un encabezado HTTP y no se guarda en la página.
La carga completa no debe programarse cada 5 minutos. Usa “Sincronizar ahora” para probar el modo incremental.
</p>

<label for="token">D1_SYNC_TOKEN</label>
<input id="token" type="password" autocomplete="off">

<button id="testBtn">Probar D1</button>
<button id="incrementalBtn">Sincronizar ahora</button>
<button id="repairReportsBtn">Reparar reportes</button>
<button id="syncBtn">Ejecutar carga inicial</button>

<pre id="out">Esperando...</pre>
</div>
</main>

<script>
const out =
  document.getElementById("out");

document
  .getElementById("testBtn")
  .onclick =
  async () => {

    out.textContent =
      "Probando conexión D1...";

    try {

      const response =
        await fetch(
          "/api/d1/test",
          {
            cache:"no-store"
          }
        );

      const data =
        await response.json();

      out.textContent =
        JSON.stringify(
          data,
          null,
          2
        );

    } catch (error) {

      out.textContent =
        String(error);

    }

  };


document
  .getElementById("incrementalBtn")
  .onclick =
  async () => {

    const token =
      document
        .getElementById("token")
        .value;

    if (!token) {

      out.textContent =
        "Ingresa D1_SYNC_TOKEN.";

      return;

    }

    out.textContent =
      "Sincronizando cambios recientes...";

    try {

      const response =
        await fetch(
          "/api/d1/sync/incremental?days=7",
          {
            method:"POST",

            headers:{
              "X-Super-Dupaza-Admin":
                token
            }
          }
        );

      const data =
        await response.json();

      out.textContent =
        JSON.stringify(
          data,
          null,
          2
        );

    } catch (error) {

      out.textContent =
        String(error);

    }

  };



document
  .getElementById("repairReportsBtn")
  .onclick =
  async () => {

    const token =
      document
        .getElementById("token")
        .value;

    if (!token) {

      out.textContent =
        "Ingresa D1_SYNC_TOKEN.";

      return;

    }


    out.textContent =
      "Reparando report_orders desde zas_orders + loaded_refs...";


    try {

      const response =
        await fetch(
          "/api/d1/report-repair",
          {
            method:
              "POST",

            headers: {
              "X-Super-Dupaza-Admin":
                token
            }
          }
        );


      const data =
        await response.json();


      out.textContent =
        JSON.stringify(
          data,
          null,
          2
        );


    } catch (error) {

      out.textContent =
        String(error);

    }

  };


document
  .getElementById("syncBtn")
  .onclick =
  async () => {

    const token =
      document
        .getElementById("token")
        .value;

    if (!token) {

      out.textContent =
        "Ingresa D1_SYNC_TOKEN.";

      return;

    }

    if (
      !confirm(
        "¿Ejecutar la carga inicial completa a D1?"
      )
    ) {
      return;
    }

    out.textContent =
      "Sincronizando ZasConnect + BO/VT hacia D1...";

    try {

      const response =
        await fetch(
          "/api/d1/sync/full",
          {
            method:"POST",

            headers:{
              "X-Super-Dupaza-Admin":
                token
            }
          }
        );

      const data =
        await response.json();

      out.textContent =
        JSON.stringify(
          data,
          null,
          2
        );

    } catch (error) {

      out.textContent =
        String(error);

    }

  };
</script>
</body>
</html>`,
    {
      headers: {
        "content-type":
          "text/html; charset=UTF-8",

        "cache-control":
          "no-store"
      }
    }
  );

}



function simpleHash(
  value
) {

  const text =
    typeof value ===
      "string"
      ? value
      : JSON.stringify(
          value
        );


  let hash =
    2166136261;


  for (
    let i = 0;
    i < text.length;
    i++
  ) {

    hash ^=
      text.charCodeAt(
        i
      );


    hash =
      Math.imul(
        hash,
        16777619
      );

  }


  return (
    hash >>>
    0
  )
    .toString(
      16
    )
    .padStart(
      8,
      "0"
    );

}




function jsonChunksByBytes(
  rows,
  maxBytes = 220000
) {

  const chunks = [];

  let current = [];

  let currentBytes = 2;


  for (
    const row
    of rows
  ) {

    const encoded =
      JSON.stringify(
        row
      );


    const rowBytes =
      new TextEncoder()
        .encode(
          encoded
        )
        .length +
      1;


    if (
      current.length &&
      currentBytes +
      rowBytes >
      maxBytes
    ) {

      chunks.push(
        current
      );


      current = [];

      currentBytes = 2;

    }


    current.push(
      row
    );


    currentBytes +=
      rowBytes;

  }


  if (
    current.length
  ) {

    chunks.push(
      current
    );

  }


  return chunks;

}



async function d1BulkInsertJson(
  db,
  table,
  columns,
  rows,
  {
    replace = false,
    maxBytes = 220000
  } = {}
) {

  // La capa PostgreSQL implementa este mismo contrato sin usar
  // json_each/json_extract de SQLite.
  if (
    db &&
    typeof db.bulkInsertJson ===
      "function"
  ) {

    return await db.bulkInsertJson(
      table,
      columns,
      rows,
      {
        replace,
        maxBytes
      }
    );

  }


  if (
    !rows ||
    !rows.length
  ) {

    return {
      rows: 0,
      queries: 0,
      chunks: 0
    };

  }


  const chunks =
    jsonChunksByBytes(
      rows,
      maxBytes
    );


  const mode =
    replace
      ? "INSERT OR REPLACE"
      : "INSERT";


  const columnSql =
    columns.join(
      ","
    );


  const selectSql =
    columns
      .map(
        column =>
          `json_extract(value, '$.${column}')`
      )
      .join(
        ","
      );


  const sql =
`${mode} INTO ${table} (
  ${columnSql}
)
SELECT
  ${selectSql}
FROM json_each(?1)`;


  let queries =
    0;


  for (
    const chunk
    of chunks
  ) {

    const payload =
      JSON.stringify(
        chunk
      );


    await db
      .prepare(
        sql
      )
      .bind(
        payload
      )
      .run();


    queries++;

  }


  return {
    rows:
      rows.length,

    queries,

    chunks:
      chunks.length
  };

}



async function d1BatchChunks(
  db,
  statements,
  chunkSize = 40
) {

  let batches =
    0;


  for (
    let i = 0;
    i < statements.length;
    i += chunkSize
  ) {

    const chunk =
      statements.slice(
        i,
        i +
        chunkSize
      );


    if (!chunk.length) {
      continue;
    }


    await db.batch(
      chunk
    );


    batches++;

  }


  return batches;

}



async function d1Test(
  env
) {

  if (!env.DB) {

    return {
      ok: false,

      error:
        "El Worker todavía no tiene un binding D1 llamado DB."
    };

  }


  const [
    orders,
    lines,
    refs,
    reports,
    sync
  ] =
    await Promise.all([

      env.DB
        .prepare(
          "SELECT COUNT(*) AS n FROM zas_orders"
        )
        .first(),

      env.DB
        .prepare(
          "SELECT COUNT(*) AS n FROM zas_order_lines"
        )
        .first(),

      env.DB
        .prepare(
          "SELECT COUNT(*) AS n FROM loaded_refs"
        )
        .first(),

      env.DB
        .prepare(
          "SELECT COUNT(*) AS n FROM report_orders"
        )
        .first(),

      env.DB
        .prepare(
          "SELECT source,last_success_at,range_from,range_to,records_seen,records_changed,last_error FROM sync_state ORDER BY source"
        )
        .all()

    ]);


  return {
    ok: true,

    binding:
      "DB",

    counts: {
      zas_orders:
        Number(
          orders?.n ||
          0
        ),

      zas_order_lines:
        Number(
          lines?.n ||
          0
        ),

      loaded_refs:
        Number(
          refs?.n ||
          0
        ),

      report_orders:
        Number(
          reports?.n ||
          0
        )
    },

    sync_state:
      sync?.results ||
      []
  };

}



function groupZasForD1(
  apiRows
) {

  const groups =
    new Map();


  for (
    const row
    of apiRows ||
    []
  ) {

    const tracking =
      normId(
        row.trackingCode
      );


    if (!tracking) {
      continue;
    }


    if (
      !groups.has(
        tracking
      )
    ) {

      groups.set(
        tracking,
        []
      );

    }


    groups
      .get(
        tracking
      )
      .push(
        row
      );

  }


  const orders = [];
  const lines = [];


  for (
    const [
      tracking,
      rows
    ]
    of groups.entries()
  ) {

    const first =
      rows[0] ||
      {};


    const orderTotal =
      Math.max(
        0,
        ...rows.map(
          row =>
            numberValue(
              row.orderTotal
            )
        )
      );


    const order = {

      tracking_code:
        tracking,

      customer_tracking_code:
        firstNonBlank(
          rows.map(
            row =>
              row.customerTrackingCode
          )
        ) ??
        null,

      tax_id:
        firstNonBlank(
          rows.map(
            row =>
              row.taxId
          )
        ) ??
        null,

      customer_name:
        firstNonBlank(
          rows.map(
            row =>
              row.customerName
          )
        ) ??
        null,

      primary_phone:
        firstNonBlank(
          rows.map(
            row =>
              row.primaryPhone
          )
        ) ??
        null,

      secondary_phone:
        firstNonBlank(
          rows.map(
            row =>
              row.secondaryPhone
          )
        ) ??
        null,

      seller_name:
        firstNonBlank(
          rows.map(
            row =>
              row.sellerName
          )
        ) ??
        null,

      shipping_municipality:
        firstNonBlank(
          rows.map(
            row =>
              row.shippingMunicipality
          )
        ) ??
        null,

      shipping_reference:
        firstNonBlank(
          rows.map(
            row =>
              row.shippingReference
          )
        ) ??
        null,

      custom_rcf:
        firstNonBlank(
          rows.map(
            row =>
              row.custom_rcf
          )
        ) ??
        null,

      sales_channel:
        firstNonBlank(
          rows.map(
            row =>
              row.salesChannel
          )
        ) ??
        null,

      shipping_department:
        firstNonBlank(
          rows.map(
            row =>
              row.shippingDepartment
          )
        ) ??
        null,

      order_date:
        dateOnly(
          firstNonBlank(
            rows.map(
              row =>
                row.orderDate
            )
          )
        ),

      comments:
        firstNonBlank(
          rows.map(
            row =>
              row.comments
          )
        ) ??
        null,

      warehouse:
        firstNonBlank(
          rows.map(
            row =>
              row.warehouse
          )
        ) ??
        null,

      order_total:
        round2(
          orderTotal
        ),

      shipping_avenue:
        firstNonBlank(
          rows.map(
            row =>
              row.shippingAvenue
          )
        ) ??
        null,

      shipping_house_number:
        firstNonBlank(
          rows.map(
            row =>
              row.shippingHouseNumber
          )
        ) ??
        null,

      shipping_zone:
        firstNonBlank(
          rows.map(
            row =>
              row.shippingZone
          )
        ) ??
        null,

      custom_bodega:
        firstNonBlank(
          rows.map(
            row =>
              row.custom_bodega
          )
        ) ??
        null,

      custom_canal:
        firstNonBlank(
          rows.map(
            row =>
              row.custom_canal
          )
        ) ??
        null,

      custom_payment_method:
        firstNonBlank(
          rows.map(
            row =>
              row.custom_payment_method
          )
        ) ??
        null,

      custom_marca:
        firstNonBlank(
          rows.map(
            row =>
              row.custom_marca
          )
        ) ??
        null,

      source_updated_at:
        firstNonBlank(
          rows.map(
            row =>
              row.updatedAt ||
              row.updated_at
          )
        ) ??
        null

    };


    order.source_hash =
      simpleHash(
        order
      );


    orders.push(
      order
    );


    const usedLineKeys =
      new Set();


    rows.forEach(
      (
        row,
        index
      ) => {

        const lineNumberRaw =
          row.lineNumber;


        const lineNumberText =
          lineNumberRaw ===
            null ||
          lineNumberRaw ===
            undefined
            ? ""
            : String(
                lineNumberRaw
              )
                .trim();


        // MUY IMPORTANTE:
        // Number(null) y Number("") devuelven 0.
        // Eso provocaba que varias filas de Excel con Línea vacía
        // terminaran todas con line_key = "LN:0" y D1 conservara
        // solamente la última.
        const lineNumber =
          lineNumberText !==
            "" &&
          Number.isFinite(
            Number(
              lineNumberText
            )
          )
            ? Number(
                lineNumberText
              )
            : null;


        const sku =
          String(
            row.sku ??
            ""
          )
            .trim();


        let lineKey =
          lineNumber !==
          null
            ? "LN:" +
              lineNumber
            : "ROW:" +
              (
                index +
                1
              ) +
              ":SKU:" +
              (
                sku ||
                "SIN-SKU"
              );


        // Incluso si el origen trae Número de línea repetido,
        // nunca permitimos que dos filas físicas de una misma orden
        // tengan la misma PK (tracking_code, line_key).
        if (
          usedLineKeys.has(
            lineKey
          )
        ) {

          lineKey +=
            ":ROW:" +
            (
              index +
              1
            ) +
            ":SKU:" +
            (
              sku ||
              "SIN-SKU"
            );

        }


        usedLineKeys.add(
          lineKey
        );


        const line = {

          tracking_code:
            tracking,

          line_key:
            lineKey,

          line_number:
            lineNumber,

          sku:
            row.sku ??
            null,

          description:
            row.description ??
            null,

          quantity:
            numberValue(
              row.quantity
            ),

          brand:
            row.brand ??
            null,

          location_code:
            row.locationCode ??
            null,

          ring_code:
            row.ringCode ??
            null,

          unit_price:
            round2(
              numberValue(
                row.unitPrice
              )
            ),

          line_total:
            round2(
              numberValue(
                row.lineTotal
              )
            )

        };


        line.source_hash =
          simpleHash(
            line
          );


        lines.push(
          line
        );

      }
    );

  }


  return {
    orders,
    lines
  };

}



function buildLoadedRefsForD1(
  detail
) {

  const preparedDetail =
    detailWithSyntheticBlankPdvIds(
      detail
    );


  const grouped =
    new Map();


  for (
    const row
    of preparedDetail
  ) {

    if (!row.uId) {
      continue;
    }


    if (
      !grouped.has(
        row.uId
      )
    ) {

      grouped.set(
        row.uId,
        []
      );

    }


    grouped
      .get(
        row.uId
      )
      .push(
        row
      );

  }


  const result = [];


  for (
    const [
      tracking,
      rows
    ]
    of grouped.entries()
  ) {

    const boRows =
      rows.filter(
        row =>
          row.source ===
          "BO"
      );


    const vtRows =
      rows.filter(
        row =>
          row.source ===
          "VT"
      );


    const pdvRows =
      rows.filter(
        row =>
          row.type ===
          "PDV"
      );


    const isPdv =
      pdvRows.length >
      0;


    const vtPdvRows =
      pdvRows.filter(
        row =>
          row.source ===
          "VT"
      );


    const boPdvRows =
      pdvRows.filter(
        row =>
          row.source ===
          "BO"
      );


    const pairedBlankOrder =
      String(
        tracking ||
        ""
      )
        .startsWith(
          "PDV-NOID|PAIR|"
        );


    const chosenRows =
      isPdv
        ? (
            pairedBlankOrder &&
            boPdvRows.length
              ? boPdvRows
              : (
                  vtPdvRows.length
                    ? vtPdvRows
                    : boPdvRows
                )
          )
        : (
            vtRows.length
              ? vtRows
              : boRows
          );


    const groupName =
      matrixGroup(
        firstNonBlank(
          chosenRows.map(
            row =>
              row.group
          )
        ) ||
        "K3"
      );


    let channel =
      null;


    if (isPdv) {

      const configuredChannel =
        firstNonBlank(
          chosenRows.map(
            row =>
              row.physicalSegment
          )
        );


      const rawGroup =
        firstNonBlank(
          chosenRows.map(
            row =>
              row.group
          )
        );


      if (
        configuredChannel ===
          "PETAPA" ||
        configuredChannel ===
          "XENACOJ"
      ) {

        channel =
          configuredChannel;

      } else if (
        rawGroup ===
        "K1 PISO XENACOJ"
      ) {

        channel =
          "XENACOJ";

      } else if (
        rawGroup ===
        "K1 PISO PETAPA"
      ) {

        channel =
          "PETAPA";

      } else {

        channel =
          "DIGITAL";

      }

    }


    const item = {

      tracking_code:
        tracking,

      is_loaded:
        1,

      is_pdv:
        isPdv
          ? 1
          : 0,

      has_bo:
        boRows.length
          ? 1
          : 0,

      has_vt:
        vtRows.length
          ? 1
          : 0,

      report_date:
        firstNonBlank(
          chosenRows.map(
            row =>
              row.date
          )
        ) ||
        firstNonBlank(
          rows.map(
            row =>
              row.date
          )
        ) ||
        null,

      route:
        firstNonBlank(
          chosenRows.map(
            row =>
              row.reportAdvisor
          )
        ) ||
        firstNonBlank(
          rows.map(
            row =>
              row.reportAdvisor
          )
        ) ||
        firstNonBlank(
          chosenRows.map(
            row =>
              row.route
          )
        ) ||
        firstNonBlank(
          rows.map(
            row =>
              row.route
          )
        ) ||
        null,

      channel,

      group_name:
        groupName,

      bo_amount:
        round2(
          sum(
            boRows.map(
              row =>
                row.amount
            )
          )
        ),

      vt_amount:
        round2(
          sum(
            vtRows.map(
              row =>
                row.amount
            )
          )
        ),

      chosen_amount:
        round2(
          sum(
            chosenRows.map(
              row =>
                row.amount
            )
          )
        )

    };


    item.source_hash =
      simpleHash(
        item
      );


    result.push(
      item
    );

  }


  return result;

}



function buildReportRowsForD1(
  apiRows,
  detail
) {

  const base =
    buildSalesReportBase(
      apiRows,
      detail
    );


  const apiOrders =
    buildApiOrders(
      apiRows
    );


  const sellerMap =
    new Map(
      apiOrders.map(
        row => [
          row.tracking,
          row.sellerOriginal
        ]
      )
    );


  return base.map(
    row => {

      const item = {

        tracking_code:
          row.tracking,

        report_date:
          row.date,

        advisor:
          row.advisor,

        seller_original:
          row.sellerOriginal ||
          sellerMap.get(
            row.tracking
          ) ||
          row.advisor ||
          null,

        order_type:
          row.type,

        group_name:
          row.group,

        level:
          row.level,

        channel:
          row.channel,

        amount:
          round2(
            row.amount
          ),

        source:
          row.source

      };


      item.source_hash =
        simpleHash(
          item
        );


      return item;

    }
  );

}



async function syncFullToD1(
  env
) {

  if (!env.DB) {

    throw new Error(
      "Falta el binding D1 llamado DB."
    );

  }


  const startedAt =
    new Date()
      .toISOString();


  await env.DB
    .prepare(
      `UPDATE sync_state
       SET last_started_at = ?1,
           last_error = NULL
       WHERE source IN ('ZASCONNECT','BO_VT','REPORTS')`
    )
    .bind(
      startedAt
    )
    .run();


  try {

    // La carga inicial ignora el cache actual.
    const [
      apps,
      zas
    ] =
      await Promise.all([

        fetchAppsScript(
          env,
          "all"
        ),

        getZasOrders(
          env
        )

      ]);


    const bo =
      Array.isArray(
        apps.bo
      )
        ? apps.bo
        : [];


    const vt =
      Array.isArray(
        apps.vt
      )
        ? apps.vt
        : [];


    const physicalSellerSegments =
      await getPhysicalSellerSegmentMap(
        env
      );


    const detail =
      buildDetail(
        bo,
        vt,
        physicalSellerSegments
      );


    const zasData =
      groupZasForD1(
        zas
      );


    const loadedRefs =
      buildLoadedRefsForD1(
        detail
      );


    const reportRows =
      buildReportRowsForD1(
        zas,
        detail
      );


    const now =
      new Date()
        .toISOString();


    // CARGA INICIAL:
    // todavía los reportes públicos usan el sistema anterior.
    // Por eso es seguro reconstruir D1 desde cero en esta fase.
    await env.DB.batch([

      env.DB.prepare(
        "DELETE FROM report_orders"
      ),

      env.DB.prepare(
        "DELETE FROM loaded_refs"
      ),

      env.DB.prepare(
        "DELETE FROM zas_order_lines"
      ),

      env.DB.prepare(
        "DELETE FROM zas_orders"
      )

    ]);


    // --------------------------------------------------------
    // ESCRITURA MASIVA OPTIMIZADA PARA D1 FREE
    //
    // En lugar de una consulta D1 por fila, pasamos arreglos JSON
    // y usamos json_each() dentro de SQLite/D1.
    // --------------------------------------------------------

    const writeStats = {
      deleteQueries: 4,
      zasOrdersQueries: 0,
      zasLinesQueries: 0,
      loadedRefsQueries: 0,
      reportOrdersQueries: 0,
      syncStateQueries: 0
    };


    const ordersWrite =
      await d1BulkInsertJson(
        env.DB,
        "zas_orders",
        [
          "tracking_code",
          "customer_tracking_code",
          "tax_id",
          "customer_name",
          "primary_phone",
          "secondary_phone",
          "seller_name",
          "shipping_municipality",
          "shipping_reference",
          "custom_rcf",
          "sales_channel",
          "shipping_department",
          "order_date",
          "comments",
          "warehouse",
          "order_total",
          "shipping_avenue",
          "shipping_house_number",
          "shipping_zone",
          "custom_bodega",
          "custom_canal",
          "custom_payment_method",
          "custom_marca",
          "source_hash",
          "source_updated_at",
          "synced_at"
        ],
        zasData.orders.map(
          row => ({
            ...row,
            synced_at:
              now
          })
        ),
        {
          replace:
            true
        }
      );


    writeStats.zasOrdersQueries =
      ordersWrite.queries;


    const linesWrite =
      await d1BulkInsertJson(
        env.DB,
        "zas_order_lines",
        [
          "tracking_code",
          "line_key",
          "line_number",
          "sku",
          "description",
          "quantity",
          "brand",
          "location_code",
          "ring_code",
          "unit_price",
          "line_total",
          "source_hash",
          "synced_at"
        ],
        zasData.lines.map(
          row => ({
            ...row,
            synced_at:
              now
          })
        ),
        {
          replace:
            true
        }
      );


    writeStats.zasLinesQueries =
      linesWrite.queries;


    const refsWrite =
      await d1BulkInsertJson(
        env.DB,
        "loaded_refs",
        [
          "tracking_code",
          "is_loaded",
          "is_pdv",
          "has_bo",
          "has_vt",
          "report_date",
          "route",
          "channel",
          "group_name",
          "bo_amount",
          "vt_amount",
          "chosen_amount",
          "source_hash",
          "synced_at"
        ],
        loadedRefs.map(
          row => ({
            ...row,
            synced_at:
              now
          })
        ),
        {
          replace:
            true
        }
      );


    writeStats.loadedRefsQueries =
      refsWrite.queries;


    const reportsWrite =
      await d1BulkInsertJson(
        env.DB,
        "report_orders",
        [
          "tracking_code",
          "report_date",
          "advisor",
          "seller_original",
          "order_type",
          "group_name",
          "level",
          "channel",
          "amount",
          "source",
          "source_hash",
          "synced_at"
        ],
        reportRows.map(
          row => ({
            ...row,
            synced_at:
              now
          })
        ),
        {
          replace:
            true
        }
      );


    writeStats.reportOrdersQueries =
      reportsWrite.queries;


    const reportDates =
      reportRows
        .map(
          row =>
            row.report_date
        )
        .filter(Boolean)
        .sort();


    const minDate =
      reportDates[0] ||
      null;


    const maxDate =
      reportDates[
        reportDates.length -
        1
      ] ||
      null;


    const zasMeta =
      JSON.stringify({
        apiLines:
          zas.length,

        orders:
          zasData.orders.length,

        lines:
          zasData.lines.length
      });


    const boVtMeta =
      JSON.stringify({
        lastImport:
          apps.lastImport ||
          null,

        sourceModified:
          apps.sourceModified ||
          null,

        boRows:
          bo.length,

        vtRows:
          vt.length,

        loadedRefs:
          loadedRefs.length,

        pdvBlankTrackingSupport:
          1,

        pdvAdvisorIdentitySupport:
          1,

        manualPhysicalSegmentSupport:
          1,

        staleSourceCleanupV1:
          1,

        pendingNormalizationDoesNotRenameReports:
          1,

        syntheticPdvRefs:
          loadedRefs.filter(
            row =>
              String(
                row.tracking_code ||
                ""
              )
                .startsWith(
                  "PDV-NOID|"
                )
          )
            .length
      });


    const reportMeta =
      JSON.stringify({
        reportOrders:
          reportRows.length
      });


    await env.DB.batch([

      env.DB
        .prepare(
`UPDATE sync_state
 SET last_success_at = ?1,
     range_from = ?2,
     range_to = ?3,
     records_seen = ?4,
     records_changed = ?5,
     metadata_json = ?6,
     last_error = NULL
 WHERE source = 'ZASCONNECT'`
        )
        .bind(
          now,
          minDate,
          maxDate,
          zasData.orders.length,
          zasData.orders.length,
          zasMeta
        ),

      env.DB
        .prepare(
`UPDATE sync_state
 SET last_success_at = ?1,
     range_from = ?2,
     range_to = ?3,
     records_seen = ?4,
     records_changed = ?5,
     metadata_json = ?6,
     last_error = NULL
 WHERE source = 'BO_VT'`
        )
        .bind(
          now,
          minDate,
          maxDate,
          detail.length,
          loadedRefs.length,
          boVtMeta
        ),

      env.DB
        .prepare(
`UPDATE sync_state
 SET last_success_at = ?1,
     range_from = ?2,
     range_to = ?3,
     records_seen = ?4,
     records_changed = ?5,
     metadata_json = ?6,
     last_error = NULL
 WHERE source = 'REPORTS'`
        )
        .bind(
          now,
          minDate,
          maxDate,
          reportRows.length,
          reportRows.length,
          reportMeta
        )

    ]);


    writeStats.syncStateQueries =
      3;


    writeStats.totalD1QueriesEstimated =
      writeStats.deleteQueries +
      writeStats.zasOrdersQueries +
      writeStats.zasLinesQueries +
      writeStats.loadedRefsQueries +
      writeStats.reportOrdersQueries +
      writeStats.syncStateQueries;


    return {
      ok: true,

      mode:
        "FULL_BOOTSTRAP",

      startedAt,

      finishedAt:
        now,

      source: {
        zasConnectApiLines:
          zas.length,

        boRows:
          bo.length,

        vtRows:
          vt.length
      },

      d1: {
        zas_orders:
          zasData.orders.length,

        zas_order_lines:
          zasData.lines.length,

        loaded_refs:
          loadedRefs.length,

        report_orders:
          reportRows.length
      },

      d1QueryUsage:
        writeStats,

      reportRange: {
        from:
          minDate,

        to:
          maxDate
      },

      note:
        "Carga inicial completada. Los reportes públicos todavía siguen usando la lógica anterior hasta validar D1."
    };


  } catch (error) {

    const message =
      String(
        error?.stack ||
        error
      );


    const failedAt =
      new Date()
        .toISOString();


    await env.DB
      .prepare(
`UPDATE sync_state
 SET last_error = ?1
 WHERE source IN ('ZASCONNECT','BO_VT','REPORTS')`
      )
      .bind(
        message.slice(
          0,
          4000
        )
      )
      .run();


    throw error;

  }

}


// ============================================================
// COLUMNAS ÓRDENES NO CARGADAS
// ============================================================

const NOT_LOADED_COLUMNS = [

  {
    key: "customerTrackingCode",
    label: "Tracking cliente"
  },

  {
    key: "taxId",
    label: "NIT"
  },

  {
    key: "customerName",
    label: "Cliente"
  },

  {
    key: "primaryPhone",
    label: "Teléfono principal"
  },

  {
    key: "secondaryPhone",
    label: "Teléfono secundario"
  },

  {
    key: "sellerName",
    label: "Vendedor"
  },

  {
    key: "shippingMunicipality",
    label: "Municipio"
  },

  {
    key: "shippingReference",
    label: "Referencia de entrega"
  },

  {
    key: "custom_rcf",
    label: "RCF"
  },

  {
    key: "salesChannel",
    label: "Canal de venta"
  },

  {
    key: "shippingDepartment",
    label: "Departamento"
  },

  {
    key: "orderDate",
    label: "Fecha pedido"
  },

  {
    key: "sku",
    label: "Código artículo"
  },

  {
    key: "description",
    label: "Descripción"
  },

  {
    key: "quantity",
    label: "Cantidad"
  },

  {
    key: "brand",
    label: "Marca"
  },

  {
    key: "locationCode",
    label: "Código de ubicación"
  },

  {
    key: "ringCode",
    label: "Código de anillo"
  },

  {
    key: "comments",
    label: "Comentarios"
  },

  {
    key: "trackingCode",
    label: "Tracking pedido"
  },

  {
    key: "warehouse",
    label: "Bodega"
  },

  {
    key: "orderTotal",
    label: "Total pedido",
    money: true
  },

  {
    key: "unitPrice",
    label: "Precio unitario",
    money: true
  },

  {
    key: "lineNumber",
    label: "Línea"
  },

  {
    key: "shippingAvenue",
    label: "Avenida o calle"
  },

  {
    key: "shippingHouseNumber",
    label: "Casa o lote"
  },

  {
    key: "shippingZone",
    label: "Zona"
  },

  {
    key: "lineTotal",
    label: "Total línea",
    money: true
  },

  {
    key: "custom_bodega",
    label: "Bodega"
  },

  {
    key: "custom_canal",
    label: "Canal"
  },

  {
    key: "custom_payment_method",
    label: "Forma de pago"
  },

  {
    key: "custom_marca",
    label: "Marca"
  }

];


// Campos generales que no se repiten visualmente en líneas hijas.
const ORDER_LEVEL_KEYS =
  new Set([
    "customerTrackingCode",
    "taxId",
    "customerName",
    "primaryPhone",
    "secondaryPhone",
    "sellerName",
    "shippingMunicipality",
    "shippingReference",
    "custom_rcf",
    "salesChannel",
    "shippingDepartment",
    "orderDate",
    "comments",
    "trackingCode",
    "warehouse",
    "orderTotal",
    "shippingAvenue",
    "shippingHouseNumber",
    "shippingZone",
    "custom_bodega",
    "custom_canal",
    "custom_payment_method",
    "custom_marca"
  ]);


// ============================================================
// APPS SCRIPT
// ============================================================

async function fetchAppsScript(
  env,
  resource
) {

  const url =
    new URL(
      APPS_SCRIPT_URL
    );

  url.searchParams.set(
    "resource",
    resource
  );

  url.searchParams.set(
    "token",
    env.APPS_SCRIPT_TOKEN
  );


  const response =
    await fetch(
      url,
      {
        redirect:
          "follow"
      }
    );


  const text =
    await response.text();


  if (!response.ok) {

    throw new Error(
      `Apps Script HTTP ${response.status}: ` +
      text.slice(0, 500)
    );

  }


  let data;


  try {

    data =
      JSON.parse(
        text
      );

  } catch {

    throw new Error(
      "Apps Script no devolvió JSON."
    );

  }


  if (!data.ok) {

    throw new Error(
      `Apps Script: ${
        data.error ||
        "error desconocido"
      }`
    );

  }


  return data;

}


// ============================================================
// ZASCONNECT
// ============================================================

async function loginZasConnect(env) {

  const form =
    new FormData();


  form.append(
    "_1_email",
    env.ZAS_LOGIN_EMAIL
  );


  form.append(
    "_1_password",
    env.ZAS_LOGIN_PASSWORD
  );


  form.append(
    "0",
    '[{},"$K1"]'
  );


  const response =
    await fetch(
      `${ZAS_BASE}/login`,
      {
        method:
          "POST",

        headers: {

          "Accept":
            "text/x-component",

          "Origin":
            ZAS_BASE,

          "Referer":
            `${ZAS_BASE}/login`,

          "Next-Action":
            env.ZAS_NEXT_ACTION

        },

        body:
          form,

        redirect:
          "manual"
      }
    );


  let setCookies = [];


  if (
    typeof
      response.headers.getSetCookie ===
    "function"
  ) {

    setCookies =
      response.headers
        .getSetCookie();

  } else {

    const raw =
      response.headers.get(
        "set-cookie"
      );


    if (raw) {

      setCookies = [
        raw
      ];

    }

  }


  const cookies =
    setCookies
      .map(
        value =>
          value
            .split(";")[0]
            .trim()
      )
      .filter(Boolean);


  const crm =
    cookies.find(
      value =>
        value.startsWith(
          "crm_session="
        )
    );


  if (!crm) {

    const body =
      await response.text();


    throw new Error(
      "Login ZasConnect no produjo crm_session. " +
      `HTTP=${response.status}; ` +
      "redirect=" +
      (
        response.headers.get(
          "x-action-redirect"
        ) ||
        ""
      ) +
      "; body=" +
      body.slice(
        0,
        300
      )
    );

  }


  return cookies.join(
    "; "
  );

}



async function fetchZasPage(
  cookie,
  page,
  pageSize = 25
) {

  const url =
    new URL(
      ZAS_EXPORT_URL
    );


  url.searchParams.set(
    "page",
    String(
      page
    )
  );


  url.searchParams.set(
    "pageSize",
    String(
      pageSize
    )
  );


  const response =
    await fetch(
      url.toString(),
      {
        headers: {
          "Accept":
            "application/json",

          "Cookie":
            cookie
        },

        cache:
          "no-store"
      }
    );


  if (!response.ok) {

    const text =
      await response.text();


    throw new Error(
      `ZasConnect export página ${page} HTTP ${response.status}: ` +
      text.slice(
        0,
        500
      )
    );

  }


  const raw =
    await response.json();


  const rows =
    Array.isArray(
      raw
    )
      ? raw
      : Array.isArray(
          raw?.data
        )
        ? raw.data
        : [];


  const pagination =
    raw &&
    typeof raw ===
      "object" &&
    !Array.isArray(
      raw
    )
      ? (
          raw.pagination ||
          raw.meta?.pagination ||
          {}
        )
      : {};


  return {
    rows,
    pagination
  };

}



async function getZasOrders(env) {

  const cookie =
    await loginZasConnect(
      env
    );


  // Primera página para conocer pageSize y total.
  const first =
    await fetchZasPage(
      cookie,
      1,
      25
    );


  const pageSize =
    Number(
      first.pagination?.pageSize ||
      25
    ) ||
    25;


  const total =
    Number(
      first.pagination?.total ||
      0
    ) ||
    0;


  const totalPages =
    total > 0
      ? Math.max(
          1,
          Math.ceil(
            total /
            pageSize
          )
        )
      : 1;


  const allRows = [
    ...first.rows
  ];


  // Consultar el resto de páginas.
  // Con 194 pedidos / 25 por página son 8 páginas.
  if (
    totalPages >
    1
  ) {

    const pageNumbers =
      Array.from(
        {
          length:
            totalPages -
            1
        },
        (
          _,
          index
        ) =>
          index +
          2
      );


    // Se consultan en paralelo porque todas son GET independientes.
    const remaining =
      await Promise.all(
        pageNumbers.map(
          page =>
            fetchZasPage(
              cookie,
              page,
              pageSize
            )
        )
      );


    for (
      const pageResult
      of remaining
    ) {

      allRows.push(
        ...pageResult.rows
      );

    }

  }


  // El endpoint devuelve líneas de producto, no solo pedidos.
  // Eliminamos únicamente duplicados 100% idénticos para evitar
  // repeticiones accidentales entre páginas.
  const uniqueRows = [];

  const seen =
    new Set();


  for (
    const row
    of allRows
  ) {

    const key =
      JSON.stringify(
        row
      );


    if (
      seen.has(
        key
      )
    ) {
      continue;
    }


    seen.add(
      key
    );


    uniqueRows.push(
      row
    );

  }


  return uniqueRows;

}


// ============================================================
// DIAGNÓSTICO TEMPORAL DEL API ZASCONNECT
// ============================================================

async function debugZasExport(env) {

  const cookie =
    await loginZasConnect(
      env
    );


  const first =
    await fetchZasPage(
      cookie,
      1,
      25
    );


  const pageSize =
    Number(
      first.pagination?.pageSize ||
      25
    ) ||
    25;


  const total =
    Number(
      first.pagination?.total ||
      0
    ) ||
    0;


  const totalPages =
    total > 0
      ? Math.max(
          1,
          Math.ceil(
            total /
            pageSize
          )
        )
      : 1;


  const rows =
    await getZasOrders(
      env
    );


  const parsedDateCounts = {};

  const rawDateCounts = {};

  const requestedDates =
    new Set([
      "2026-10-01",
      "2026-10-03",
      "2026-10-04"
    ]);


  const targetTrackings = [];


  for (
    const row
    of rows
  ) {

    const rawDate =
      String(
        row?.orderDate ??
        ""
      )
        .trim();


    const parsed =
      dateOnly(
        rawDate
      );


    if (rawDate) {

      rawDateCounts[
        rawDate
      ] =
        (
          rawDateCounts[
            rawDate
          ] ||
          0
        ) +
        1;

    }


    if (parsed) {

      parsedDateCounts[
        parsed
      ] =
        (
          parsedDateCounts[
            parsed
          ] ||
          0
        ) +
        1;


      if (
        requestedDates.has(
          parsed
        )
      ) {

        targetTrackings.push({
          trackingCode:
            row?.trackingCode ??
            null,

          sellerName:
            row?.sellerName ??
            null,

          rawOrderDate:
            rawDate,

          parsedOrderDate:
            parsed,

          orderTotal:
            row?.orderTotal ??
            null
        });

      }

    }

  }


  const parsedDates =
    Object.keys(
      parsedDateCounts
    )
      .sort();


  return {
    ok: true,

    pagination: {
      pageSize,
      total,
      totalPages
    },

    rowsReceivedAllPages:
      rows.length,

    minParsedDate:
      parsedDates[0] ||
      null,

    maxParsedDate:
      parsedDates[
        parsedDates.length -
        1
      ] ||
      null,

    requestedDates: {
      "2026-10-01":
        parsedDateCounts[
          "2026-10-01"
        ] ||
        0,

      "2026-10-03":
        parsedDateCounts[
          "2026-10-03"
        ] ||
        0,

      "2026-10-04":
        parsedDateCounts[
          "2026-10-04"
        ] ||
        0
    },

    parsedDateCounts,

    rawDateCounts,

    targetSamples:
      targetTrackings
        .slice(
          0,
          50
        )
  };

}


// ============================================================
// CACHE DE FUENTES
// ============================================================

async function getSources(
  env,
  ctx,
  force = false
) {

  const cache =
    /** @type {any} */
    (caches).default;


  const key =
    new Request(
      "https://super-dupaza.internal/source-cache-final-v2"
    );


  if (!force) {

    const hit =
      await cache.match(
        key
      );


    if (hit) {

      return await hit.json();

    }

  }


  const [
    apps,
    zas
  ] =
    await Promise.all([
      fetchAppsScript(
        env,
        "all"
      ),

      getZasOrders(
        env
      )
    ]);


  const payload = {

    bo:
      Array.isArray(
        apps.bo
      )
        ? apps.bo
        : [],

    vt:
      Array.isArray(
        apps.vt
      )
        ? apps.vt
        : [],

    zas,

    appsStatus: {
      ok: true,

      lastImport:
        apps.lastImport ??
        null
    },

    generatedAt:
      new Date()
        .toISOString()

  };


  const response =
    new Response(
      JSON.stringify(
        payload
      ),
      {
        headers: {

          "content-type":
            "application/json",

          "cache-control":
            `public, max-age=${SOURCE_CACHE_SECONDS}`

        }
      }
    );


  ctx.waitUntil(
    cache.put(
      key,
      response
    )
  );


  return payload;

}


// ============================================================
// NORMALIZACIÓN
// ============================================================

function normText(value) {

  return String(
    value ??
    ""
  )
    .trim()
    .toUpperCase()
    .normalize("NFD")
    .replace(
      /[\u0300-\u036f]/g,
      ""
    )
    .replace(
      /\s+/g,
      " "
    );

}



function normId(value) {

  return String(
    value ??
    ""
  )
    .trim()
    .toUpperCase();

}



function normalizeRoute(value) {

  const key =
    normText(
      value
    );


  return (
    NAME_MAP.get(
      key
    ) ||
    key
  );

}



function numberValue(value) {

  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {

    return 0;

  }


  const n =
    Number(
      String(value)
        .replace(
          /,/g,
          ""
        )
    );


  return Number.isFinite(
    n
  )
    ? n
    : 0;

}



function round2(value) {

  return Math.round(
    (
      Number(value) +
      Number.EPSILON
    ) *
    100
  ) /
  100;

}



function sum(values) {

  return values.reduce(
    (
      total,
      value
    ) =>
      total +
      numberValue(
        value
      ),
    0
  );

}



function firstNonBlank(values) {

  for (
    const value
    of values
  ) {

    const text =
      String(
        value ??
        ""
      )
        .trim();


    if (text) {

      return value;

    }

  }


  return null;

}



function dateOnly(value) {

  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {
    return null;
  }

  const text =
    String(value)
      .trim();

  // ISO / SQL: 2026-10-01 o 2026-10-01T...
  const iso =
    text.match(
      /^(\d{4})-(\d{1,2})-(\d{1,2})/
    );

  if (iso) {

    const year =
      iso[1];

    const month =
      String(
        Number(iso[2])
      ).padStart(
        2,
        "0"
      );

    const day =
      String(
        Number(iso[3])
      ).padStart(
        2,
        "0"
      );

    return (
      year +
      "-" +
      month +
      "-" +
      day
    );
  }

  // ZasConnect / Guatemala: DD/MM/YYYY o DD-MM-YYYY.
  // Nunca se interpreta como MM/DD/YYYY.
  const guatemala =
    text.match(
      /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(?:\D|$)/
    );

  if (guatemala) {

    const dayNumber =
      Number(
        guatemala[1]
      );

    const monthNumber =
      Number(
        guatemala[2]
      );

    const yearNumber =
      Number(
        guatemala[3]
      );

    if (
      dayNumber >= 1 &&
      dayNumber <= 31 &&
      monthNumber >= 1 &&
      monthNumber <= 12
    ) {

      const day =
        String(
          dayNumber
        ).padStart(
          2,
          "0"
        );

      const month =
        String(
          monthNumber
        ).padStart(
          2,
          "0"
        );

      return (
        String(
          yearNumber
        ) +
        "-" +
        month +
        "-" +
        day
      );
    }
  }

  // Último recurso únicamente para timestamps inequívocos.
  const d =
    new Date(
      text
    );

  if (
    Number.isNaN(
      d.getTime()
    )
  ) {
    return null;
  }

  return d
    .toISOString()
    .slice(
      0,
      10
    );
}


// ============================================================
// NORMALIZACIÓN FUZZY - SOLO PENDIENTES DE CARGA
// ============================================================

function normalizeMatchText(value) {

  return String(value ?? "")
    .trim()
    .toUpperCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

}


function normalizeGeoMatchText(value) {

  let text =
    String(value ?? "")
      .trim()
      .toUpperCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");


  // Los textos entre paréntesis del catálogo solo sirven para
  // desambiguar; el departamento se compara por separado.
  text =
    text.replace(/\([^)]*\)/g, " ");


  text =
    text
      .replace(/\bDEPARTAMENTO\s+DE\b/g, " ")
      .replace(/\bDEPARTAMENTO\b/g, " ")
      .replace(/\bDEPTO\.?\s+DE\b/g, " ")
      .replace(/\bDEPTO\.?\b/g, " ")
      .replace(/\bMUNICIPIO\s+DE\b/g, " ")
      .replace(/\bMUNICIPIO\b/g, " ")
      .replace(/\bMUN\.?\b/g, " ")
      .replace(/\bSAC\.?\b/g, " SACATEPEQUEZ ")
      .replace(/[^A-Z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();


  // Zona 1 -> Zona 01 para comparar con el catálogo oficial.
  text =
    text.replace(
      /\bZONA\s+([0-9])\b/g,
      "ZONA 0$1"
    );


  return text;

}


function levenshteinDistance(a, b) {

  if (a === b) return 0;

  if (!a.length) return b.length;

  if (!b.length) return a.length;


  let previous =
    Array.from(
      { length: b.length + 1 },
      (_, i) => i
    );


  let current =
    new Array(
      b.length + 1
    );


  for (
    let i = 1;
    i <= a.length;
    i++
  ) {

    current[0] = i;


    for (
      let j = 1;
      j <= b.length;
      j++
    ) {

      const cost =
        a[i - 1] === b[j - 1]
          ? 0
          : 1;


      current[j] =
        Math.min(
          current[j - 1] + 1,
          previous[j] + 1,
          previous[j - 1] + cost
        );

    }


    const swap =
      previous;

    previous =
      current;

    current =
      swap;

  }


  return previous[
    b.length
  ];

}


function similarityRatio(a, b) {

  const left =
    String(a ?? "");

  const right =
    String(b ?? "");


  if (
    !left ||
    !right
  ) {
    return 0;
  }


  if (
    left === right
  ) {
    return 1;
  }


  const maxLength =
    Math.max(
      left.length,
      right.length
    );


  return (
    1 -
    levenshteinDistance(
      left,
      right
    ) /
    maxLength
  );

}


function normalizePendingSeller(
  value,
  nameRules =
    PENDING_NAME_NORMALIZATION
) {

  const raw =
    String(value ?? "")
      .trim();


  if (!raw) {
    return raw;
  }


  const input =
    normalizeMatchText(
      raw
    );


  let bestCanonical =
    raw;

  let bestScore =
    0;


  for (
    const entry
    of (
      nameRules ||
      PENDING_NAME_NORMALIZATION
    )
  ) {

    const candidates =
      [
        entry.canonical,
        ...(
          entry.aliases ||
          []
        )
      ]
        .filter(Boolean);


    for (
      const alias
      of candidates
    ) {

      const score =
        similarityRatio(
          input,
          normalizeMatchText(
            alias
          )
        );


      if (
        score >
        bestScore
      ) {

        bestScore =
          score;

        bestCanonical =
          entry.canonical;

      }


      if (
        score === 1
      ) {

        return entry.canonical;

      }

    }

  }


  return (
    bestScore >=
    PENDING_MATCH_THRESHOLD
  )
    ? bestCanonical
    : raw;

}


function bestPendingDepartment(value) {

  const raw =
    String(value ?? "")
      .trim();


  const input =
    normalizeGeoMatchText(
      raw
    );


  if (!input) {

    return {
      value: raw,
      score: 0
    };

  }


  let best =
    raw;

  let bestScore =
    0;


  for (
    const department
    of PENDING_DEPARTMENTS
  ) {

    const score =
      similarityRatio(
        input,
        normalizeGeoMatchText(
          department
        )
      );


    if (
      score >
      bestScore
    ) {

      bestScore =
        score;

      best =
        department;

    }


    if (
      score === 1
    ) {
      break;
    }

  }


  return {
    value: best,
    score: bestScore
  };

}



function exactPendingDepartment(
  value
) {

  const raw =
    String(
      value ??
      ""
    )
      .trim();


  const key =
    normalizeGeoMatchText(
      raw
    );


  if (!key) {

    return {
      value:
        raw,
      matched:
        false
    };

  }


  const found =
    PENDING_DEPARTMENTS.find(
      department =>
        normalizeGeoMatchText(
          department
        ) ===
        key
    );


  return {
    value:
      found ||
      raw,

    matched:
      Boolean(
        found
      )
  };

}



function pendingZoneSuffix(
  value
) {

  const raw =
    String(
      value ??
      ""
    )
      .trim();


  if (!raw) {
    return null;
  }


  const normalized =
    normalizeGeoMatchText(
      raw
    );


  const match =
    normalized.match(
      /(?:^|\s)ZONA\s+(\d{1,2})(?:\s|$)/
    ) ||
    normalized.match(
      /^(\d{1,2})$/
    );


  if (!match) {
    return null;
  }


  return (
    "ZONA " +
    String(
      Number(
        match[1]
      )
    )
      .padStart(
        2,
        "0"
      )
  );

}



function normalizePendingLocation(
  municipality,
  department,
  zone = null
) {

  const rawMunicipality =
    String(
      municipality ??
      ""
    )
      .trim();


  const rawDepartment =
    String(
      department ??
      ""
    )
      .trim();


  const departmentMatch =
    exactPendingDepartment(
      rawDepartment
    );


  const departmentKey =
    normalizeGeoMatchText(
      departmentMatch.value
    );


  const candidates =
    departmentMatch.matched
      ? PENDING_MUNICIPALITIES.filter(
          item =>
            normalizeGeoMatchText(
              item.department
            ) ===
            departmentKey
        )
      : PENDING_MUNICIPALITIES;


  // La tabla MunName / DepName es un CATÁLOGO, no una tabla de homólogos.
  // La comparación del municipio es exacta después de normalizar:
  // mayúsculas/minúsculas, acentos, espacios y Zona 1 = Zona 01.
  // NO se usa fuzzy matching para municipios.
  const lookupValues = [];


  const zoneSuffix =
    pendingZoneSuffix(
      zone
    );


  // Si el API separa municipio y zona, primero se compara la combinación.
  // Ejemplo: Guatemala + 18 => Guatemala Zona 18.
  if (
    rawMunicipality &&
    zoneSuffix &&
    !/\bZONA\s+\d{1,2}\b/i.test(
      rawMunicipality
    )
  ) {

    lookupValues.push(
      rawMunicipality +
      " " +
      zoneSuffix
    );

  }


  if (rawMunicipality) {

    lookupValues.push(
      rawMunicipality
    );

  }


  for (
    const lookupValue
    of lookupValues
  ) {

    const lookupKey =
      normalizeGeoMatchText(
        lookupValue
      );


    const found =
      candidates.find(
        item =>
          normalizeGeoMatchText(
            item.municipality
          ) ===
          lookupKey
      );


    if (found) {

      return {
        municipality:
          found.municipality,

        department:
          found.department
      };

    }

  }


  // Sin coincidencia exacta: conservar lo que vino del API.
  return {
    municipality:
      rawMunicipality,

    department:
      departmentMatch.matched
        ? departmentMatch.value
        : rawDepartment
  };

}


function normalizePendingApiRow(
  row,
  nameRules =
    PENDING_NAME_NORMALIZATION
) {

  const location =
    normalizePendingLocation(
      row.shippingMunicipality,
      row.shippingDepartment,
      row.shippingZone
    );


  const sellerOriginal =
    String(
      row.sellerName ??
      ""
    )
      .trim();


  const sellerNormalized =
    normalizePendingSeller(
      sellerOriginal,
      nameRules
    );


  return {
    ...row,

    // En pendientes seguimos mostrando/copiendo el nombre normalizado,
    // pero conservamos el vendedor REAL recibido desde ZasConnect para
    // mostrarlo en el tooltip al pasar el mouse.
    _sellerOriginal:
      sellerOriginal,

    sellerName:
      sellerNormalized,

    shippingMunicipality:
      location.municipality,

    shippingDepartment:
      location.department
  };

}


// ============================================================
// BO / VT
// ============================================================

function classify(
  type,
  route
) {

  const tipo =
    normText(
      type
    );


  const ruta =
    normalizeRoute(
      route
    );


  if (
    tipo === "PDV" &&
    K1_PETAPA.has(
      ruta
    )
  ) {

    return "K1 PISO PETAPA";

  }


  if (
    tipo === "PDV" &&
    K1_XENACOJ.has(
      ruta
    )
  ) {

    return "K1 PISO XENACOJ";

  }


  if (
    DIGITAL_K2.has(
      ruta
    )
  ) {

    return "K2 DIGITAL Y K2";

  }


  return "K3";

}



function matrixGroup(group) {

  if (
    group ===
      "K1 PISO PETAPA" ||
    group ===
      "K1 PISO XENACOJ"
  ) {

    return "K1 PISO";

  }


  if (
    group ===
    "K2 DIGITAL Y K2"
  ) {

    return "K2 DIGITAL Y K2";

  }


  return "K3";

}




async function getPhysicalSellerSegmentMap(
  env
) {

  await ensureReportRulesSchema(
    env
  );


  const result =
    await env.DB
      .prepare(
`SELECT
  n.canonical_name,
  n.alias1,
  n.alias2,
  n.alias3,
  n.alias4,
  n.alias5,
  c.segment

FROM pending_name_normalization AS n

JOIN seller_report_rule_rows AS c
  ON c.normalization_id =
     n.id

WHERE c.segment IN (
  'PETAPA',
  'XENACOJ'
)

ORDER BY n.id`
      )
      .all();


  const map =
    new Map();


  for (
    const row
    of (
      result?.results ||
      []
    )
  ) {

    const segment =
      validReportSegment(
        row.segment
      );


    if (
      segment !==
        "PETAPA" &&
      segment !==
        "XENACOJ"
    ) {
      continue;
    }


    const candidates = [
      row.canonical_name,
      row.alias1,
      row.alias2,
      row.alias3,
      row.alias4,
      row.alias5
    ];


    for (
      const value
      of candidates
    ) {

      const key =
        normText(
          value
        );


      if (!key) {
        continue;
      }


      // Si el mismo nombre aparece en una fila DIGITAL y otra PETAPA,
      // únicamente estamos leyendo filas físicas en esta consulta.
      // Para BO/VT esto permite detectar tienda física aunque TIPO venga vacío.
      if (
        !map.has(
          key
        )
      ) {

        map.set(
          key,
          segment
        );

      }

    }

  }


  return map;

}



function buildDetail(
  boRows,
  vtRows,
  physicalSellerSegments =
    new Map()
) {

  const rows = [];


  for (
    const row
    of boRows ||
    []
  ) {

    const reportAdvisor =
      String(
        row.Ruta ??
        ""
      )
        .trim();


    const route =
      normalizeRoute(
        reportAdvisor
      );


    const rawType =
      normText(
        row.TIPO
      );


    const configuredPhysicalSegment =
      physicalSellerSegments.get(
        normText(
          reportAdvisor
        )
      ) ||
      physicalSellerSegments.get(
        normText(
          route
        )
      ) ||
      null;


    const type =
      (
        rawType ===
          "PDV" ||
        configuredPhysicalSegment
      )
        ? "PDV"
        : rawType;


    rows.push({

      source:
        "BO",

      document:
        row.DocEntry ??
        null,

      // Identidad auxiliar para detectar una misma orden BO/VT cuando
      // todavía no tiene U_IDFRONT. Si Apps Script trae columnas más
      // completas, se aprovechan; BO FC actual conserva Tel1 como respaldo.
      customerRaw:
        firstNonBlank([
          row.Cliente,
          row.ClienteNombre,
          row.Tel1
        ]),

      productRaw:
        firstNonBlank([
          row.Producto,
          row.Descripcion,
          row.ItemCode,
          row.SKU,
          row.CodigoArticulo
        ]),

      date:
        dateOnly(
          row.FechaCreado
        ),

      route,

      reportAdvisor:
        reportAdvisor ||
        route,

      physicalSegment:
        configuredPhysicalSegment,

      type,

      uId:
        normId(
          row.U_IDFRONT
        ),

      // BO viene sin IVA.
      amount:
        round2(
          numberValue(
            row.BO
          ) *
          1.12
        ),

      units:
        numberValue(
          row.UBO
        ),

      group:
        classify(
          type,
          route
        )

    });

  }


  for (
    const row
    of vtRows ||
    []
  ) {

    const reportAdvisor =
      String(
        row.Ruta ??
        ""
      )
        .trim();


    const route =
      normalizeRoute(
        reportAdvisor
      );


    const rawType =
      normText(
        row.TIPO
      );


    const configuredPhysicalSegment =
      physicalSellerSegments.get(
        normText(
          reportAdvisor
        )
      ) ||
      physicalSellerSegments.get(
        normText(
          route
        )
      ) ||
      null;


    const type =
      (
        rawType ===
          "PDV" ||
        configuredPhysicalSegment
      )
        ? "PDV"
        : rawType;


    rows.push({

      source:
        "VT",

      document:
        row.NumSAP ??
        null,

      customerRaw:
        firstNonBlank([
          row.Cliente,
          row.ClienteNombre,
          row.Tel1
        ]),

      productRaw:
        firstNonBlank([
          row.Producto,
          row.Descripcion,
          row.ItemCode,
          row.SKU,
          row.CodigoArticulo
        ]),

      date:
        dateOnly(
          row.Fecha
        ),

      route,

      reportAdvisor:
        reportAdvisor ||
        route,

      physicalSegment:
        configuredPhysicalSegment,

      type,

      uId:
        normId(
          row.U_IDFRONT
        ),

      // VT viene sin IVA.
      amount:
        round2(
          numberValue(
            row.VT
          ) *
          1.12
        ),

      units:
        numberValue(
          row.UVT
        ),

      group:
        classify(
          type,
          route
        )

    });

  }


  return rows;

}




function boVtCustomerKey(
  row
) {

  const raw =
    String(
      row?.customerRaw ??
      ""
    )
      .trim();


  if (!raw) {
    return "";
  }


  // BO FC suele traer Tel1 y VT FC suele traer "número - nombre".
  // Cuando existe un identificador numérico razonable lo usamos para
  // permitir la comparación cruzada sin depender de mayúsculas/acentos.
  const numeric =
    raw.match(
      /\d{7,}/g
    );


  if (
    numeric &&
    numeric.length
  ) {

    return (
      "N:" +
      numeric[0]
        .replace(
          /\D/g,
          ""
        )
    );

  }


  return (
    "T:" +
    normText(
      raw
    )
  );

}


function boVtProductKey(
  row
) {

  const raw =
    String(
      row?.productRaw ??
      ""
    )
      .trim();


  if (!raw) {
    return "";
  }


  // Si viene "CODIGO - DESCRIPCION", el código es la identidad más estable.
  const code =
    raw
      .split(
        /\s+-\s+/
      )[0]
      .trim();


  return normText(
    code ||
    raw
  );

}


function sameBlankPdvOrderAcrossBoVt(
  bo,
  vt
) {

  if (
    bo?.source !==
      "BO" ||
    vt?.source !==
      "VT" ||
    bo?.type !==
      "PDV" ||
    vt?.type !==
      "PDV" ||
    bo?.uId ||
    vt?.uId
  ) {
    return false;
  }


  const boSeller =
    normText(
      bo.reportAdvisor ||
      bo.route
    );


  const vtSeller =
    normText(
      vt.reportAdvisor ||
      vt.route
    );


  if (
    boSeller &&
    vtSeller &&
    boSeller !==
      vtSeller
  ) {
    return false;
  }


  if (
    Math.abs(
      numberValue(
        bo.amount
      ) -
      numberValue(
        vt.amount
      )
    ) >
    0.01
  ) {
    return false;
  }


  const boCustomer =
    boVtCustomerKey(
      bo
    );


  const vtCustomer =
    boVtCustomerKey(
      vt
    );


  // Sin una identidad de cliente comparable no hacemos una deduplicación
  // arriesgada. Es preferible conservar dos ventas que unir dos clientes
  // distintos por casualidad.
  if (
    !boCustomer ||
    !vtCustomer ||
    boCustomer !==
      vtCustomer
  ) {
    return false;
  }


  const boProduct =
    boVtProductKey(
      bo
    );


  const vtProduct =
    boVtProductKey(
      vt
    );


  if (
    boProduct &&
    vtProduct &&
    boProduct !==
      vtProduct
  ) {
    return false;
  }


  // Si no tenemos producto en ambos orígenes, exigimos además la misma
  // fecha para reducir falsos positivos. Cuando sí hay cliente+producto
  // exactos se permite que BO FechaCreado y VT Fecha sean diferentes.
  if (
    (!boProduct ||
     !vtProduct) &&
    bo.date &&
    vt.date &&
    bo.date !==
      vt.date
  ) {
    return false;
  }


  const boUnits =
    numberValue(
      bo.units
    );


  const vtUnits =
    numberValue(
      vt.units
    );


  if (
    boUnits >
      0 &&
    vtUnits >
      0 &&
    Math.abs(
      boUnits -
      vtUnits
    ) >
      0.001
  ) {
    return false;
  }


  return true;

}


function detailWithSyntheticBlankPdvIds(
  detail
) {

  const sourceRows =
    detail ||
    [];


  const blankBo =
    sourceRows.filter(
      row =>
        !row.uId &&
        row.type ===
          "PDV" &&
        row.source ===
          "BO"
    );


  const blankVt =
    sourceRows.filter(
      row =>
        !row.uId &&
        row.type ===
          "PDV" &&
        row.source ===
          "VT"
    );


  // Pares BO/VT detectados como la MISMA orden sin tracking.
  // Se marcan con un mismo ID y más abajo se contabilizan una sola vez.
  const pairedIds =
    new Map();


  const usedVt =
    new Set();


  for (
    const bo
    of blankBo
  ) {

    const vt =
      blankVt.find(
        candidate =>
          !usedVt.has(
            candidate
          ) &&
          sameBlankPdvOrderAcrossBoVt(
            bo,
            candidate
          )
      );


    if (!vt) {
      continue;
    }


    usedVt.add(
      vt
    );


    const pairId =
      "PDV-NOID|PAIR|" +
      simpleHash({
        boDocument:
          normId(
            bo.document
          ),
        vtDocument:
          normId(
            vt.document
          ),
        customer:
          boVtCustomerKey(
            bo
          ),
        product:
          boVtProductKey(
            bo
          ) ||
          boVtProductKey(
            vt
          ),
        advisor:
          normText(
            bo.reportAdvisor ||
            bo.route ||
            vt.reportAdvisor ||
            vt.route
          ),
        amount:
          round2(
            numberValue(
              bo.amount
            )
          ),
        boDate:
          bo.date ||
          null,
        vtDate:
          vt.date ||
          null
      });


    pairedIds.set(
      bo,
      pairId
    );


    pairedIds.set(
      vt,
      pairId
    );

  }


  return sourceRows.map(
    row => {

      if (
        row.uId ||
        row.type !==
          "PDV"
      ) {
        return row;
      }


      const pairedId =
        pairedIds.get(
          row
        );


      if (pairedId) {

        return {
          ...row,
          uId:
            pairedId
        };

      }


      const source =
        row.source ===
          "BO"
          ? "BO"
          : "VT";


      const document =
        normId(
          row.document
        );


      if (document) {

        return {
          ...row,

          // Cada documento PDV sin tracking se conserva como una orden
          // independiente. Esto evita que dos BO del mismo vendedor/día
          // se colapsen en una sola venta.
          uId:
            "PDV-NOID|" +
            source +
            "|DOC:" +
            document
        };

      }


      const fallbackFingerprint =
        simpleHash({
          source,
          date:
            row.date ||
            null,
          advisor:
            normText(
              row.reportAdvisor ||
              row.route ||
              "SIN-ASIGNAR"
            ),
          customer:
            boVtCustomerKey(
              row
            ),
          product:
            boVtProductKey(
              row
            ),
          amount:
            round2(
              numberValue(
                row.amount
              )
            ),
          units:
            numberValue(
              row.units
            )
        });


      return {
        ...row,

        uId:
          "PDV-NOID|" +
          source +
          "|ROW:" +
          fallbackFingerprint
      };

    }
  );

}



function buildTrackingReference(
  detail
) {

  const preparedDetail =
    detailWithSyntheticBlankPdvIds(
      detail
    );


  const grouped =
    new Map();


  for (
    const row
    of preparedDetail
  ) {

    if (!row.uId) {
      continue;
    }


    if (
      !grouped.has(
        row.uId
      )
    ) {

      grouped.set(
        row.uId,
        []
      );

    }


    grouped
      .get(
        row.uId
      )
      .push(
        row
      );

  }


  const refs =
    new Map();


  for (
    const [
      tracking,
      rows
    ]
    of grouped.entries()
  ) {

    const pdvRows =
      rows.filter(
        item =>
          item.type ===
          "PDV"
      );


    const isPdv =
      pdvRows.length >
      0;


    const anyVt =
      rows.find(
        item =>
          item.source ===
            "VT" &&
          item.date
      );


    const anyBo =
      rows.find(
        item =>
          item.source ===
            "BO" &&
          item.date
      );


    let pdvDate =
      null;

    let pdvAmount =
      0;

    let pdvRoute =
      null;

    let pdvAdvisor =
      null;

    let pdvGroup =
      null;

    let pdvChannel =
      null;


    if (isPdv) {

      const vtPdv =
        pdvRows.filter(
          item =>
            item.source ===
            "VT"
        );


      const boPdv =
        pdvRows.filter(
          item =>
            item.source ===
            "BO"
        );


      // Si la misma orden SIN tracking fue detectada en BO y VT,
      // se contabiliza una sola vez y BO es el origen canónico para usar
      // FechaCreado. Para trackings reales, ZasConnect será el dueño final.
      const pairedBlankOrder =
        String(
          tracking ||
          ""
        )
          .startsWith(
            "PDV-NOID|PAIR|"
          );


      const chosen =
        pairedBlankOrder &&
        boPdv.length
          ? boPdv
          : (
              vtPdv.length
                ? vtPdv
                : boPdv
            );


      pdvDate =
        firstNonBlank(
          chosen.map(
            item =>
              item.date
          )
        );


      pdvAmount =
        round2(
          sum(
            chosen.map(
              item =>
                item.amount
            )
          )
        );


      pdvRoute =
        firstNonBlank(
          chosen.map(
            item =>
              item.route
          )
        );


      pdvAdvisor =
        firstNonBlank(
          chosen.map(
            item =>
              item.reportAdvisor
          )
        ) ||
        pdvRoute;


      pdvGroup =
        firstNonBlank(
          chosen.map(
            item =>
              item.group
          )
        );


      const configuredChannel =
        firstNonBlank(
          chosen.map(
            item =>
              item.physicalSegment
          )
        );


      if (
        configuredChannel ===
          "PETAPA" ||
        configuredChannel ===
          "XENACOJ"
      ) {

        pdvChannel =
          configuredChannel;

      } else if (
        pdvGroup ===
        "K1 PISO XENACOJ"
      ) {

        pdvChannel =
          "XENACOJ";

      } else if (
        pdvGroup ===
        "K1 PISO PETAPA"
      ) {

        pdvChannel =
          "PETAPA";

      } else {

        pdvChannel =
          "DIGITAL";

      }

    }


    refs.set(
      tracking,
      {

        isPdv,

        anyDate:
          anyVt?.date ||
          anyBo?.date ||
          null,

        pdvDate,

        pdvAmount,

        pdvRoute,

        pdvAdvisor,

        pdvGroup,

        pdvChannel

      }
    );

  }


  return refs;

}


// ============================================================
// ZASCONNECT -> UNA FILA POR ORDEN
// ============================================================

function buildApiOrders(
  apiRows
) {

  const groups =
    new Map();


  for (
    const row
    of apiRows ||
    []
  ) {

    const tracking =
      normId(
        row.trackingCode
      );


    if (!tracking) {
      continue;
    }


    if (
      !groups.has(
        tracking
      )
    ) {

      groups.set(
        tracking,
        []
      );

    }


    groups
      .get(
        tracking
      )
      .push(
        row
      );

  }


  const result = [];


  for (
    const [
      tracking,
      rows
    ]
    of groups.entries()
  ) {

    const sellerOriginal =
      normText(
        firstNonBlank(
          rows.map(
            item =>
              item.sellerName
          )
        )
      );


    // El vendedor del API se conserva como vendedor real.
    // Juan Mejia y Glenda Reinoso nunca se mezclan.
    const sellerDisplay =
      normalizeRoute(
        sellerOriginal
      );


    // Los hijos pueden repetir/traer 0.
    // Se toma el máximo orderTotal una sola vez por tracking.
    const amount =
      Math.max(
        0,
        ...rows.map(
          item =>
            numberValue(
              item.orderTotal
            )
        )
      );


    result.push({

      tracking,

      advisor:
        sellerDisplay ||
        "SIN ASIGNAR",

      sellerOriginal,

      explicitPdvPetapa:
        false,

      date:
        dateOnly(
          firstNonBlank(
            rows.map(
              item =>
                item.orderDate
            )
          )
        ),

      amount:
        round2(
          amount
        )

    });

  }


  return result;

}



function apiGroupFromSeller(
  sellerOriginal
) {

  const seller =
    normText(
      sellerOriginal
    );


  if (
    API_PDV_PETAPA_SELLERS.has(
      seller
    )
  ) {

    return "K1 PISO";

  }


  const normalized =
    normalizeRoute(
      seller
    );


  return DIGITAL_K2.has(
    normalized
  )
    ? "K2 DIGITAL Y K2"
    : "K3";

}


// ============================================================
// BASE ÚNICA DE REPORTES
// ============================================================

function buildSalesReportBase(
  apiRows,
  detail
) {

  const apiOrders =
    buildApiOrders(
      apiRows
    );


  const refs =
    buildTrackingReference(
      detail
    );


  const used =
    new Set();


  const rows = [];


  for (
    const order
    of apiOrders
  ) {

    used.add(
      order.tracking
    );


    const ref =
      refs.get(
        order.tracking
      );


    const explicitPdv =
      order.explicitPdvPetapa;


    const matchedPdv =
      Boolean(
        ref?.isPdv
      );


    const isPdv =
      explicitPdv ||
      matchedPdv;


    let channel;
    let group;
    let reportDate;

    let reportAdvisor =
      order.advisor;

    let reportSellerOriginal =
      order.sellerOriginal;

    let reportAmount =
      order.amount;

    let reportSource =
      "ZASCONNECT";


    if (explicitPdv) {

      channel =
        "PETAPA";

      group =
        "K1 PISO";


      // Si existe una orden ZasConnect, la fecha del reporte
      // SIEMPRE viene de ZasConnect.
      reportDate =
        order.date ||
        ref?.anyDate ||
        null;


    } else if (
      matchedPdv
    ) {

      // IMPORTANTE:
      // Que un tracking exista también en BO/VT NO convierte al vendedor
      // del API al segmento/ruta que aparece en BO/VT.
      //
      // Para una orden con tracking ZasConnect:
      // - identidad y monto: ZasConnect
      // - fecha: ZasConnect
      // - K y segmento: regla del vendedor en ⚙ NOMBRES
      //
      // Aquí dejamos una base neutra de API. sellerReportEffectiveCteSql()
      // aplica después K/segmento según seller_report_match.
      channel =
        "DIGITAL";


      group =
        apiGroupFromSeller(
          order.sellerOriginal
        );


      reportDate =
        order.date ||
        null;


      reportAdvisor =
        order.advisor;


      reportSellerOriginal =
        order.sellerOriginal;


      reportAmount =
        order.amount;


      reportSource =
        "ZASCONNECT";


    } else {

      channel =
        "DIGITAL";


      group =
        apiGroupFromSeller(
          order.sellerOriginal
        );


      // Venta normal usa fecha ZasConnect.
      reportDate =
        order.date;

    }


    const level =
      group ===
        "K1 PISO"
        ? "K1"
        : group ===
            "K2 DIGITAL Y K2"
          ? "K2"
          : "K3";


    rows.push({

      tracking:
        order.tracking,

      advisor:
        reportAdvisor,

      sellerOriginal:
        reportSellerOriginal,

      date:
        reportDate,

      type:
        isPdv
          ? "PDV"
          : "DIGITAL",

      group,

      level,

      channel,

      amount:
        reportAmount,

      source:
        reportSource

    });

  }


  // PDV que existe en BO/VT pero todavía no en ZasConnect.
  for (
    const [
      tracking,
      ref
    ]
    of refs.entries()
  ) {

    const noUidSynthetic =
      String(
        tracking ||
        ""
      )
        .startsWith(
          "PDV-NOID|"
        );


    if (
      used.has(
        tracking
      ) ||
      !ref.isPdv ||
      !noUidSynthetic
    ) {

      continue;

    }


    const group =
      matrixGroup(
        ref.pdvGroup ||
        "K3"
      );


    const level =
      group ===
        "K1 PISO"
        ? "K1"
        : group ===
            "K2 DIGITAL Y K2"
          ? "K2"
          : "K3";


    const pdvOnlyAdvisor =
      ref.pdvAdvisor ||
      ref.pdvRoute ||
      "SIN ASIGNAR";


    rows.push({

      tracking,

      advisor:
        pdvOnlyAdvisor,

      sellerOriginal:
        normText(
          pdvOnlyAdvisor
        ) ||
        pdvOnlyAdvisor,

      date:
        ref.pdvDate,

      type:
        "PDV",

      group,

      level,

      channel:
        ref.pdvChannel ||
        "DIGITAL",

      amount:
        round2(
          ref.pdvAmount
        ),

      source:
        "PDV BO/VT"

    });

  }


  return rows;

}


// ============================================================
// ÓRDENES NO CARGADAS
// ============================================================

function buildNotLoaded(
  apiRows,
  detail,
  nameRules =
    PENDING_NAME_NORMALIZATION
) {

  const pdvIds =
    new Set(
      detail
        .filter(
          item =>
            item.type ===
              "PDV" &&
            item.uId
        )
        .map(
          item =>
            item.uId
        )
    );


  const loadedNonPdv =
    new Set(
      detail
        .filter(
          item =>
            item.type !==
              "PDV" &&
            item.uId
        )
        .map(
          item =>
            item.uId
        )
    );


  const notLoadedTrackings =
    new Set();


  for (
    const row
    of apiRows ||
    []
  ) {

    const tracking =
      normId(
        row.trackingCode
      );


    if (!tracking) {
      continue;
    }


    if (
      !pdvIds.has(
        tracking
      ) &&
      !loadedNonPdv.has(
        tracking
      )
    ) {

      notLoadedTrackings.add(
        tracking
      );

    }

  }


  const filtered =
    (
      apiRows ||
      []
    )
      .filter(
        row =>
          notLoadedTrackings.has(
            normId(
              row.trackingCode
            )
          )
      )
      // IMPORTANTE:
      // la normalización de nombres/municipios/departamentos
      // se aplica ÚNICAMENTE a las pendientes de carga.
      .map(
        row =>
          normalizePendingApiRow(
            row,
            nameRules
          )
      );


  const skuMap =
    new Map();


  for (
    const row
    of filtered
  ) {

    const tracking =
      normId(
        row.trackingCode
      );


    const sku =
      String(
        row.sku ??
        ""
      )
        .trim();


    if (
      !skuMap.has(
        tracking
      )
    ) {

      skuMap.set(
        tracking,
        []
      );

    }


    const list =
      skuMap.get(
        tracking
      );


    if (
      sku &&
      !list.some(
        existing =>
          normText(
            existing
          ) ===
          normText(
            sku
          )
      )
    ) {

      list.push(
        sku
      );

    }

  }


  const seen =
    new Set();


  const rows =
    filtered.map(
      row => {

        const tracking =
          normId(
            row.trackingCode
          );


        const first =
          !seen.has(
            tracking
          );


        seen.add(
          tracking
        );


        return {

          ...row,

          _firstOrderRow:
            first,

          _copySkus:
            (
              skuMap.get(
                tracking
              ) ||
              []
            )
              .join(
                "\n"
              )

        };

      }
    );


  const orderTotals = [];


  for (
    const tracking
    of notLoadedTrackings
  ) {

    const amounts =
      filtered
        .filter(
          row =>
            normId(
              row.trackingCode
            ) ===
            tracking
        )
        .map(
          row =>
            numberValue(
              row.orderTotal
            )
        );


    orderTotals.push(
      Math.max(
        0,
        ...amounts
      )
    );

  }


  const totalLines =
    round2(
      sum(
        filtered.map(
          row =>
            row.lineTotal
        )
      )
    );


  return {

    rows,

    kpis: {

      orders:
        notLoadedTrackings.size,

      lines:
        rows.length,

      total:
        round2(
          sum(
            orderTotals
          )
        ),

      totalLines

    }

  };

}



function buildNotLoadedOptions(
  rows
) {

  const unique =
    (
      values
    ) =>
      [
        ...new Set(
          values
            .map(
              value =>
                String(
                  value ??
                  ""
                )
                  .trim()
            )
            .filter(Boolean)
        )
      ]
        .sort(
          (
            a,
            b
          ) =>
            a.localeCompare(
              b,
              "es"
            )
        );


  return {

    sellers:
      unique(
        rows.map(
          row =>
            row.sellerName
        )
      ),

    departments:
      unique(
        rows.map(
          row =>
            row.shippingDepartment
        )
      ),

    municipalities:
      unique(
        rows.map(
          row =>
            row.shippingMunicipality
        )
      ),

    channels:
      unique(
        rows.map(
          row =>
            row.custom_canal ||
            row.salesChannel
        )
      ),

    payments:
      unique(
        rows.map(
          row =>
            row.custom_payment_method
        )
      ),

    brands:
      unique(
        rows.map(
          row =>
            row.custom_marca ||
            row.brand
        )
      ),

    warehouses:
      unique(
        rows.map(
          row =>
            row.warehouse ||
            row.custom_bodega
        )
      )

  };

}


// ============================================================
// FILTROS REPORTES
// ============================================================

function applyDateFilter(
  rows,
  params
) {

  const from =
    params.get(
      "date_from"
    );


  const to =
    params.get(
      "date_to"
    );


  return rows.filter(
    row => {

      if (
        from &&
        (
          !row.date ||
          row.date <
          from
        )
      ) {

        return false;

      }


      if (
        to &&
        (
          !row.date ||
          row.date >
          to
        )
      ) {

        return false;

      }


      return true;

    }
  );

}



function applyReportFilters(
  rows,
  params
) {

  const dateFiltered =
    applyDateFilter(
      rows,
      params
    );


  const group =
    params.get(
      "group"
    ) ||
    "";


  const advisor =
    params.get(
      "advisor"
    ) ||
    "";


  const type =
    params.get(
      "type"
    ) ||
    "";


  const source =
    params.get(
      "source"
    ) ||
    "";


  return dateFiltered.filter(
    row => {

      if (
        group &&
        row.group !==
        group
      ) {

        return false;

      }


      if (
        advisor &&
        row.advisor !==
        advisor
      ) {

        return false;

      }


      if (
        type &&
        row.type !==
        type
      ) {

        return false;

      }


      if (
        source &&
        row.source !==
        source
      ) {

        return false;

      }


      return true;

    }
  );

}



function buildReportOptions(
  rows
) {

  const unique =
    values =>
      [
        ...new Set(
          values
            .filter(Boolean)
        )
      ]
        .sort(
          (
            a,
            b
          ) =>
            String(a)
              .localeCompare(
                String(b),
                "es"
              )
        );


  const dates =
    rows
      .map(
        row =>
          row.date
      )
      .filter(Boolean)
      .sort();


  return {

    groups:
      unique(
        rows.map(
          row =>
            row.group
        )
      ),

    advisors:
      unique(
        rows.map(
          row =>
            row.advisor
        )
      ),

    types:
      unique(
        rows.map(
          row =>
            row.type
        )
      ),

    sources:
      unique(
        rows.map(
          row =>
            row.source
        )
      ),

    minDate:
      dates[0] ||
      null,

    maxDate:
      dates[
        dates.length -
        1
      ] ||
      null

  };

}


// ============================================================
// MATRIZ
// ============================================================

function buildMatrix(base) {

  const groups = [
    "K1 PISO",
    "K2 DIGITAL Y K2",
    "K3"
  ];


  const result = [];


  for (
    const group
    of groups
  ) {

    const row = {

      grupo:
        group,

      DIGITAL:
        0,

      PETAPA:
        0,

      XENACOJ:
        0,

      TOTAL:
        0

    };


    for (
      const item
      of base.filter(
        value =>
          value.group ===
          group
      )
    ) {

      if (
        CHANNELS.includes(
          item.channel
        )
      ) {

        row[
          item.channel
        ] +=
          item.amount;

      }

    }


    row.DIGITAL =
      round2(
        row.DIGITAL
      );


    row.PETAPA =
      round2(
        row.PETAPA
      );


    row.XENACOJ =
      round2(
        row.XENACOJ
      );


    row.TOTAL =
      round2(
        row.DIGITAL +
        row.PETAPA +
        row.XENACOJ
      );


    result.push(
      row
    );

  }


  result.push({

    grupo:
      "RESUMEN",

    DIGITAL:
      round2(
        sum(
          result.map(
            row =>
              row.DIGITAL
          )
        )
      ),

    PETAPA:
      round2(
        sum(
          result.map(
            row =>
              row.PETAPA
          )
        )
      ),

    XENACOJ:
      round2(
        sum(
          result.map(
            row =>
              row.XENACOJ
          )
        )
      ),

    TOTAL:
      round2(
        sum(
          result.map(
            row =>
              row.TOTAL
          )
        )
      )

  });


  return result;

}


// ============================================================
// GRÁFICO DIARIO
// ============================================================

function buildDailyChart(
  base
) {

  const grouped =
    new Map();


  for (
    const row
    of base
  ) {

    if (!row.date) {
      continue;
    }


    if (
      !grouped.has(
        row.date
      )
    ) {

      grouped.set(
        row.date,
        {

          date:
            row.date,

          K1:
            0,

          K2:
            0,

          K3:
            0

        }
      );

    }


    grouped
      .get(
        row.date
      )[
        row.level
      ] +=
      row.amount;

  }


  return [
    ...grouped.values()
  ]
    .sort(
      (
        a,
        b
      ) =>
        a.date.localeCompare(
          b.date
        )
    )
    .map(
      row => {

        const total =
          row.K1 +
          row.K2 +
          row.K3;


        return {

          date:
            row.date,

          K1:
            total
              ? round2(
                  row.K1 /
                  total *
                  100
                )
              : 0,

          K2:
            total
              ? round2(
                  row.K2 /
                  total *
                  100
                )
              : 0,

          K3:
            total
              ? round2(
                  row.K3 /
                  total *
                  100
                )
              : 0,

          amountK1:
            round2(
              row.K1
            ),

          amountK2:
            round2(
              row.K2
            ),

          amountK3:
            round2(
              row.K3
            ),

          total:
            round2(
              total
            )

        };

      }
    );

}


// ============================================================
// KPI RESUMEN
// ============================================================

function buildSummaryKpis(
  base
) {

  const zas =
    sum(
      base
        .filter(
          row =>
            row.source ===
            "ZASCONNECT"
        )
        .map(
          row =>
            row.amount
        )
    );


  const pdvExtra =
    sum(
      base
        .filter(
          row =>
            row.source ===
            "PDV BO/VT"
        )
        .map(
          row =>
            row.amount
        )
    );


  return {

    zasConnect:
      round2(
        zas
      ),

    pdvAdded:
      round2(
        pdvExtra
      ),

    total:
      round2(
        zas +
        pdvExtra
      ),

    orders:
      base.length

  };

}


// ============================================================
// RESUMEN POR ASESOR
// ============================================================

function buildAdvisorSummary(
  base
) {

  const map =
    new Map();


  for (
    const row
    of base
  ) {

    if (
      !map.has(
        row.advisor
      )
    ) {

      map.set(
        row.advisor,
        {

          ASESOR:
            row.advisor,

          MONTO:
            0,

          DIGITAL:
            0,

          PETAPA:
            0,

          XENACOJ:
            0

        }
      );

    }


    const obj =
      map.get(
        row.advisor
      );


    if (
      CHANNELS.includes(
        row.channel
      )
    ) {

      obj[
        row.channel
      ] +=
        row.amount;

    }


    obj.MONTO +=
      row.amount;

  }


  const rows =
    [
      ...map.values()
    ]
      .map(
        row => ({

          ASESOR:
            row.ASESOR,

          MONTO:
            round2(
              row.MONTO
            ),

          DIGITAL:
            round2(
              row.DIGITAL
            ),

          PETAPA:
            round2(
              row.PETAPA
            ),

          XENACOJ:
            round2(
              row.XENACOJ
            )

        })
      )
      .sort(
        (
          a,
          b
        ) =>
          b.MONTO -
          a.MONTO ||
          a.ASESOR.localeCompare(
            b.ASESOR,
            "es"
          )
      );


  rows.push({

    ASESOR:
      "TOTAL",

    MONTO:
      round2(
        sum(
          rows.map(
            row =>
              row.MONTO
          )
        )
      ),

    DIGITAL:
      round2(
        sum(
          rows.map(
            row =>
              row.DIGITAL
          )
        )
      ),

    PETAPA:
      round2(
        sum(
          rows.map(
            row =>
              row.PETAPA
          )
        )
      ),

    XENACOJ:
      round2(
        sum(
          rows.map(
            row =>
              row.XENACOJ
          )
        )
      )

  });


  return rows;

}



function buildAdvisorKpis(
  base
) {

  return {

    advisors:
      new Set(
        base.map(
          row =>
            row.advisor
        )
      )
        .size,

    total:
      round2(
        sum(
          base.map(
            row =>
              row.amount
          )
        )
      ),

    digital:
      round2(
        sum(
          base
            .filter(
              row =>
                row.channel ===
                "DIGITAL"
            )
            .map(
              row =>
                row.amount
            )
        )
      ),

    petapa:
      round2(
        sum(
          base
            .filter(
              row =>
                row.channel ===
                "PETAPA"
            )
            .map(
              row =>
                row.amount
            )
        )
      ),

    xenacoj:
      round2(
        sum(
          base
            .filter(
              row =>
                row.channel ===
                "XENACOJ"
            )
            .map(
              row =>
                row.amount
            )
        )
      )

  };

}


// ============================================================
// RESPUESTA JSON
// ============================================================

function json(
  data,
  status = 200
) {

  return Response.json(
    data,
    {
      status,

      headers: {
        "Cache-Control":
          "no-store"
      }
    }
  );

}


// ============================================================
// DASHBOARD COMPLETO
// ============================================================

function htmlDashboard() {

  return new Response(
String.raw`<!DOCTYPE html>
<html lang="es">

<head>

<meta charset="UTF-8">

<meta
  name="viewport"
  content="width=device-width,initial-scale=1"
>

<title>SUPER DUPAZA ONLINE</title>

<style>

:root{

  --black:#131313;
  --black2:#232323;

  --yellow:#f4c400;
  --yellow2:#ffdc3d;

  --bg:#f4f6f8;

  --card:#ffffff;

  --line:#e2e6ea;

  --text:#202124;

  --muted:#667085;

  --blue:#4f81bd;

  --red:#c0504d;

  --green:#9bbb59;

  --ok:#027a48;

  --okbg:#ecfdf3;

  --bad:#b42318;

  --badbg:#fef3f2;

  --warn:#b54708;

  --warnbg:#fffaeb;

}


*{
  box-sizing:border-box
}


body{

  margin:0;

  font-family:
    Segoe UI,
    Arial,
    sans-serif;

  background:
    var(--bg);

  color:
    var(--text);

}


header{

  position:sticky;

  top:0;

  z-index:50;

  display:flex;

  align-items:center;

  justify-content:space-between;

  gap:16px;

  padding:
    15px 24px;

  color:#fff;

  background:
    var(--black);

  border-bottom:
    4px solid
    var(--yellow);

}


.brand{

  display:flex;

  align-items:center;

  gap:12px;

}


.logo{

  width:44px;

  height:44px;

  display:grid;

  place-items:center;

  border-radius:10px;

  color:#111;

  background:
    var(--yellow);

  font-weight:900;

}


.brand h1{

  margin:0;

  font-size:20px;

}


.brand small{

  color:#cfcfcf;

}


button{

  border:0;

  border-radius:8px;

  padding:
    10px 13px;

  font-weight:750;

  cursor:pointer;

}


button:disabled{

  opacity:.55;

  cursor:not-allowed;

}


.primary{

  color:#111;

  background:
    var(--yellow);

}


.dark{

  color:#fff;

  background:#353535;

}


.light{

  color:#222;

  background:#fff;

  border:
    1px solid
    #d0d5dd;

}


.container{

  max-width:1850px;

  margin:auto;

  padding:
    20px 24px
    40px;

}


.statusbar{

  display:flex;

  gap:18px;

  flex-wrap:wrap;

  margin-bottom:16px;

  padding:
    11px 14px;

  font-size:13px;

  color:
    var(--muted);

  background:#fff;

  border:
    1px solid
    var(--line);

  border-radius:11px;

}


.dot{

  width:9px;

  height:9px;

  display:inline-block;

  margin-right:5px;

  border-radius:50%;

  background:#aaa;

}


.dot.ok{
  background:#12b76a
}


.dot.bad{
  background:#f04438
}


.notice{

  display:none;

  margin-bottom:14px;

  padding:
    11px 13px;

  border-radius:9px;

  font-size:13px;

}


.notice.show{
  display:block
}


.notice.ok{

  color:
    var(--ok);

  background:
    var(--okbg);

}


.notice.bad{

  color:
    var(--bad);

  background:
    var(--badbg);

}


.notice.warn{

  color:
    var(--warn);

  background:
    var(--warnbg);

}


.tabs{

  display:flex;

  gap:8px;

  flex-wrap:wrap;

  margin-bottom:16px;

}


.tab{

  color:#222;

  background:#fff;

  border:
    1px solid
    var(--line);

}


.tab.active{

  color:#fff;

  background:
    var(--black);

  border-color:
    var(--black);

}


.view{
  display:none
}


.view.active{
  display:block
}


.cards{

  display:grid;

  grid-template-columns:
    repeat(
      4,
      minmax(
        170px,
        1fr
      )
    );

  gap:12px;

  margin-bottom:16px;

}


.card{

  padding:15px;

  background:#fff;

  border:
    1px solid
    var(--line);

  border-radius:12px;

}


.card .label{

  font-size:11px;

  font-weight:800;

  color:
    var(--muted);

  text-transform:uppercase;

}


.card .value{

  margin-top:6px;

  font-size:25px;

  font-weight:900;

}


.card .sub{

  margin-top:3px;

  font-size:11px;

  color:
    var(--muted);

}


.panel{

  margin-bottom:16px;

  padding:15px;

  background:#fff;

  border:
    1px solid
    var(--line);

  border-radius:12px;

}


.panel-title{

  display:flex;

  align-items:center;

  justify-content:space-between;

  gap:12px;

  margin-bottom:13px;

}


.panel-title h2{

  margin:0;

  font-size:17px;

}


.small{
  font-size:12px
}


.muted{
  color:var(--muted)
}


.filters{

  display:grid;

  grid-template-columns:
    repeat(
      6,
      minmax(
        145px,
        1fr
      )
    );

  gap:9px;

  margin-bottom:13px;

}


.filters .wide{
  grid-column:span 2
}


.report-primary-controls{

  display:flex;

  align-items:flex-end;

  gap:8px;

  flex-wrap:wrap;

  margin-bottom:13px;

  padding:10px 12px;

  border:1px solid #e5e7eb;

  border-radius:10px;

  background:#fff;

}


.report-primary-controls label{

  display:flex;

  flex-direction:column;

  gap:4px;

  min-width:165px;

  font-size:11px;

  font-weight:900;

  color:#667085;

}


.report-primary-controls input[type="date"]{

  width:100%;

}


.report-primary-controls button{

  min-height:38px;

}


.report-filter-panel{

  display:none;

  margin-bottom:13px;

  padding:12px;

  border:1px solid #e5e7eb;

  border-radius:10px;

  background:#fafafa;

}


.report-filter-panel.show{
  display:block;
}


.filter-panel-actions{

  display:flex;

  gap:6px;

  flex-wrap:wrap;

  margin-top:8px;

}


.filter-toggle.active{

  background:#202020;

  color:#fff;

  border-color:#202020;

}


.range-status{

  margin-top:5px;

  font-size:11px;

  color:#667085;

}


input[type="date"]{
  cursor:pointer;
}


input,
select{

  width:100%;

  padding:
    9px 10px;

  background:#fff;

  border:
    1px solid
    #d0d5dd;

  border-radius:8px;

}


.table-wrap{

  max-height:65vh;

  overflow:auto;

  border:
    1px solid
    var(--line);

  border-radius:9px;

}


table{

  width:100%;

  min-width:max-content;

  border-collapse:separate;

  border-spacing:0;

  font-size:12.5px;

}


th,
td{

  padding:
    9px 10px;

  white-space:nowrap;

  text-align:left;

  border-bottom:
    1px solid
    #eee;

  border-right:
    1px solid
    #f1f1f1;

}


th{

  position:sticky;

  top:0;

  z-index:3;

  color:#fff;

  background:#202020;

}


tbody tr:hover td{
  background:#fffbea
}


.money{

  text-align:right;

  font-variant-numeric:
    tabular-nums;

}


.strong{
  font-weight:900
}



/* ÓRDENES NO CARGADAS: encabezados legibles.
   El <tr> real del encabezado es #ordersHead. */
#ordersHead th{
  color:#111827 !important;
  font-weight:900 !important;
  text-shadow:none !important;
}

/* Refuerzo por el contenedor real del reporte. */
#notLoaded #ordersHead th{
  color:#111827 !important;
}

.child-row{

  display:table-row !important;

  visibility:visible !important;

  opacity:1 !important;

}


.child-row td{

  color:#475467;

  background:#fafafa;

}


.line-position{

  display:inline-block;

  padding:2px 5px;

  border-radius:999px;

  background:#eef2f6;

  color:#475467;

  font-size:10px;

  font-weight:800;

  white-space:nowrap;

}


#ordersBody td{

  cursor:copy;

  user-select:none;

}


#ordersBody td:hover{

  outline-offset:-2px;

}



.report-config-card{

  width:
    min(
      92vw,
      850px
    );

}


.report-label-grid{

  display:grid;

  grid-template-columns:
    repeat(
      3,
      minmax(
        160px,
        1fr
      )
    );

  gap:
    12px;

  margin-top:
    16px;

}


.report-label-grid label{

  display:flex;

  flex-direction:column;

  gap:
    5px;

  font-size:
    12px;

  font-weight:
    800;

}


.report-label-grid input{

  width:100%;

  padding:
    9px;

  border:
    1px solid
    #d0d5dd;

  border-radius:
    6px;

}


@media(max-width:700px){

  .report-label-grid{

    grid-template-columns:
      1fr;

  }

}


.name-normalization-card{

  width:
    min(
      96vw,
      1500px
    );

  max-height:
    90vh;

}


.name-rule-help{

  margin:
    12px 0;

  padding:
    10px 12px;

  border-radius:
    8px;

  background:
    #fff8d7;

  font-size:
    13px;

  line-height:
    1.4;

}


.name-rules-table-wrap{

  overflow:auto;

  max-height:
    62vh;

  border:
    1px solid
    #e5e7eb;

  border-radius:
    10px;

}


.name-rules-table{

  width:100%;

  min-width:
    1480px;

  border-collapse:
    collapse;

  table-layout:
    fixed;

}


.name-rules-table th{

  position:
    sticky;

  top:0;

  z-index:3;

  background:
    #202020;

  color:
    #fff;

  font-size:
    11px;

  padding:
    8px;

}


.name-rules-table td{

  padding:
    6px;

  border-bottom:
    1px solid
    #e5e7eb;

  vertical-align:
    middle;

}


.name-rules-table input,
.name-rules-table select{

  width:100%;

  min-width:0;

  box-sizing:
    border-box;

  padding:
    8px;

  border:
    1px solid
    #d0d5dd;

  border-radius:
    6px;

  font-size:
    12px;

}


.name-rule-actions{

  display:flex;

  gap:5px;

  justify-content:
    center;

}


.name-rule-actions button{

  min-width:
    68px;

  padding:
    7px 8px;

}


.name-rule-new td{

  background:
    #fffdf0;

}



.manual-api-import-card{
  width:min(96vw,1100px);
  max-height:92vh;
}

.manual-api-import-drop{
  border:2px dashed #c9ced6;
  border-radius:12px;
  padding:18px;
  background:#fafafa;
  margin:12px 0;
}

.manual-api-import-drop input{
  width:100%;
}

.manual-api-import-kpis{
  display:grid;
  grid-template-columns:repeat(5,minmax(120px,1fr));
  gap:8px;
  margin:12px 0;
}

.manual-api-import-kpi{
  border:1px solid #e5e7eb;
  border-radius:9px;
  padding:10px;
  background:#fff;
}

.manual-api-import-kpi b{
  display:block;
  font-size:18px;
  margin-top:4px;
}

.manual-api-import-preview{
  max-height:280px;
  overflow:auto;
  border:1px solid #e5e7eb;
  border-radius:9px;
}

.manual-api-import-preview table{
  width:100%;
  border-collapse:collapse;
  font-size:12px;
}

.manual-api-import-preview th{
  position:sticky;
  top:0;
  background:#202020;
  color:#fff;
  padding:7px;
  text-align:left;
}

.manual-api-import-preview td{
  padding:7px;
  border-bottom:1px solid #eee;
}

.manual-api-import-status{
  margin-top:10px;
  padding:10px;
  border-radius:8px;
  background:#f5f7fa;
  white-space:pre-wrap;
}

@media(max-width:800px){
  .manual-api-import-kpis{
    grid-template-columns:repeat(2,minmax(120px,1fr));
  }
}

.seller-target-card{

  width:min(900px,96vw);

  max-height:90vh;

}


.seller-target-toolbar{

  display:grid;

  grid-template-columns:180px 1fr;

  gap:10px;

  margin-bottom:12px;

}


.seller-target-table-wrap{

  max-height:58vh;

  overflow:auto;

  border:1px solid #e5e7eb;

  border-radius:10px;

}


.seller-target-table{
  min-width:620px;
}


.seller-target-table input{

  min-width:150px;

  text-align:right;

}


@media(max-width:650px){

  .seller-target-toolbar{
    grid-template-columns:1fr;
  }

}


/* ==========================================================
   ÓRDENES NO CARGADAS — CELDAS COMPACTAS + TOOLTIP COMPLETO
   ========================================================== */

#notLoaded .table-wrap{

  max-height:none;

  overflow:visible;

}


#notLoaded table{

  width:100%;

  min-width:0;

  table-layout:fixed;

  font-size:
    clamp(
      6px,
      .62vw,
      9px
    );

}


#notLoaded th,
#notLoaded td{

  width:auto;

  height:
    var(
      --pending-cell-size,
      38px
    );

  min-width:0;

  max-width:none;

  padding:2px;

  white-space:nowrap;

  overflow:hidden;

  text-overflow:ellipsis;

  line-height:1.05;

  vertical-align:middle;

  text-align:center;

}


#notLoaded th{

  position:sticky;

  top:0;

  z-index:25;

  background:#f3f4f6;

  box-shadow:
    0 2px 0
    rgba(0,0,0,.12);

  white-space:normal;

  overflow:hidden;

}


#ordersBody td:not(.sap-action-cell){

  cursor:copy;

}


#ordersBody td:not(.sap-action-cell):hover{

  background:#fff8d6;

  outline:
    2px solid
    var(--yellow);

  outline-offset:-2px;

}


/* ----------------------------------------------------------
   ÓRDENES NO CARGADAS - PEDIDOS QUE NO SON DE HOY (GUATEMALA)
   ---------------------------------------------------------- */

#ordersBody tr.pending-old-order td{

  background:
    #fff0ee;

  color:
    #7a271a;

  border-bottom-color:
    #f5b7ae;

}


#ordersBody tr.pending-old-order td:first-child{

  box-shadow:
    inset 4px 0 0
    #d92d20;

}


#ordersBody tr.pending-old-order.child-row td{

  background:
    #fff5f3;

  color:
    #8a3426;

}


#ordersBody tr.pending-old-order:hover td{

  background:
    #ffe1dc;

}


#ordersBody tr.pending-old-order .sap-action-btn{

  background:
    #f97066;

  color:
    #fff;

}


.old-order-legend{

  display:inline-block;

  padding:
    3px 7px;

  border:
    1px solid
    #f5b7ae;

  border-radius:
    999px;

  color:
    #b42318;

  background:
    #fff0ee;

  font-weight:
    800;

}



#notLoaded .sap-action-cell{

  text-align:center;

  vertical-align:middle;

}


.sap-action-btn{

  width:100%;

  height:100%;

  min-width:0;

  padding:2px;

  border:0;

  border-radius:5px;

  background:
    var(--yellow);

  color:#161616;

  font-size:
    clamp(
      6px,
      .58vw,
      9px
    );

  font-weight:900;

  line-height:1.02;

  cursor:pointer;

  white-space:normal;

  overflow:hidden;

}


.sap-action-btn:hover{

  filter:brightness(.96);

}


.cell-tooltip{

  display:none;

  position:fixed;

  z-index:3000;

  min-width:180px;

  max-width:min(440px,calc(100vw - 24px));

  max-height:280px;

  overflow:auto;

  padding:10px 12px;

  color:#161616;

  background:#fff;

  border:
    2px solid
    var(--yellow);

  border-radius:9px;

  box-shadow:
    0 12px 34px
    rgba(0,0,0,.30);

  white-space:pre-wrap;

  overflow-wrap:anywhere;

  font-size:14px;

  line-height:1.35;

  pointer-events:none;

}


.cell-tooltip.show{

  display:block;

}


.field-modal{

  display:none;

  position:fixed;

  inset:0;

  z-index:2500;

  padding:24px;

  background:
    rgba(0,0,0,.45);

  align-items:center;

  justify-content:center;

}


.field-modal.show{

  display:flex;

}


.field-modal-card{

  width:min(760px,96vw);

  max-height:88vh;

  overflow:auto;

  padding:18px;

  background:#fff;

  border-radius:14px;

  box-shadow:
    0 18px 50px
    rgba(0,0,0,.28);

}


.field-modal-head{

  display:flex;

  align-items:flex-start;

  justify-content:space-between;

  gap:12px;

  margin-bottom:14px;

}


.field-modal-head h3{

  margin:0 0 5px;

}


.field-config-list{

  display:grid;

  grid-template-columns:
    repeat(
      auto-fit,
      minmax(190px,1fr)
    );

  gap:8px;

}


.field-option{

  display:flex;

  align-items:center;

  gap:8px;

  min-height:36px;

  padding:7px 9px;

  border:1px solid #e5e7eb;

  border-radius:8px;

  background:#fafafa;

  cursor:pointer;

}


.field-option input{

  width:auto;

  margin:0;

}


.field-modal-actions{

  display:flex;

  justify-content:flex-end;

  flex-wrap:wrap;

  gap:8px;

  margin-top:16px;

  padding-top:14px;

  border-top:1px solid #e5e7eb;

}


.manual-status{

  display:inline-block;

  padding:4px 7px;

  border-radius:999px;

  font-size:11px;

  font-weight:800;

  background:#fff4bf;

}


.manual-status.resolved{

  background:#dcfce7;

  color:#166534;

}


#manualSap table{

  min-width:0;

  table-layout:fixed;

}


#manualSap th,
#manualSap td{

  white-space:normal;

  overflow-wrap:anywhere;

  vertical-align:top;

}


@media (max-width:900px){

  #notLoaded table{

    font-size:6px;

  }

}


.copy-toast{

  position:fixed;

  right:18px;

  bottom:18px;

  z-index:1000;

  padding:
    11px 15px;

  color:#fff;

  background:#131313;

  border-left:
    5px solid
    var(--yellow);

  border-radius:8px;

  box-shadow:
    0 8px 25px
    rgba(0,0,0,.22);

  font-size:13px;

  font-weight:700;

  opacity:0;

  transform:
    translateY(8px);

  pointer-events:none;

  transition:
    .18s ease;

}


.copy-toast.show{

  opacity:1;

  transform:
    translateY(0);

}


.matrix-wrap{

  width:100%;

  overflow:hidden;

}


.matrix{

  width:100%;

  min-width:0;

  table-layout:fixed;

  font-size:
    clamp(
      9px,
      1.8vw,
      13px
    );

}


.matrix th,
.matrix td{

  padding:
    clamp(
      5px,
      1vw,
      9px
    )
    clamp(
      3px,
      .8vw,
      10px
    );

  white-space:normal;

  overflow-wrap:anywhere;

}


.matrix th{
  text-align:center
}


.matrix td:first-child{
  font-weight:800
}


.matrix td:not(:first-child){

  text-align:right;

  font-weight:700;

}


.matrix tr.total td{

  color:#111;

  background:
    var(--yellow);

  font-weight:900;

}


.report-grid{

  display:flex;

  flex-direction:column;

  gap:16px;

  width:100%;

}


.chart-card{

  width:100%;

  padding:14px;

  background:#fff;

  border:
    1px solid
    var(--line);

  border-radius:12px;

}


.chart-legend{

  display:flex;

  justify-content:center;

  gap:14px;

  flex-wrap:wrap;

  margin:
    4px 0 10px;

  font-size:12px;

}


.legend-dot{

  width:11px;

  height:11px;

  display:inline-block;

  margin-right:5px;

}


.legend-k1{
  background:var(--blue)
}


.legend-k2{
  background:var(--red)
}


.legend-k3{
  background:var(--green)
}


.chart-scroll{

  overflow-x:auto;

  padding:
    2px 2px 8px;

}


.chart-layout{

  display:grid;

  grid-template-columns:
    34px
    46px
    minmax(
      650px,
      1fr
    );

  grid-template-rows:
    320px
    24px
    24px;

  min-width:760px;

}


.y-axis-title{

  grid-column:1;

  grid-row:1;

  display:flex;

  align-items:center;

  justify-content:center;

  writing-mode:
    vertical-rl;

  transform:
    rotate(180deg);

  color:#475467;

  font-size:12px;

  font-weight:800;

}


.y-ticks{

  grid-column:2;

  grid-row:1;

  display:flex;

  flex-direction:column;

  justify-content:space-between;

  align-items:flex-end;

  padding:
    0 7px 0 0;

  color:#667085;

  font-size:10px;

}


.plot-area{

  position:relative;

  grid-column:3;

  grid-row:1;

  border-left:
    2px solid
    #344054;

  border-bottom:
    2px solid
    #344054;

  background:
    linear-gradient(
      to bottom,
      transparent 24.7%,
      #e4e7ec 25%,
      transparent 25.3%,
      transparent 49.7%,
      #e4e7ec 50%,
      transparent 50.3%,
      transparent 74.7%,
      #e4e7ec 75%,
      transparent 75.3%
    );

}


.stack-chart{

  position:absolute;

  inset:0;

  display:flex;

  align-items:flex-end;

  gap:18px;

  min-width:max-content;

  padding:
    12px 18px 0;

}


.bar-col{

  display:flex;

  flex-direction:column;

  align-items:center;

  justify-content:flex-end;

  min-width:64px;

  height:100%;

}


.bar-stack{

  display:flex;

  flex-direction:column-reverse;

  justify-content:flex-start;

  width:54px;

  height:280px;

  overflow:hidden;

  border-radius:
    3px 3px 0 0;

  box-shadow:
    1px 1px 2px
    rgba(0,0,0,.18);

}


.segment{

  width:100%;

  display:flex;

  align-items:center;

  justify-content:center;

  min-height:0;

  color:#fff;

  font-size:10px;

  font-weight:800;

  text-shadow:
    0 1px 2px
    #444;

}


.segment.k1{
  background:var(--blue)
}


.segment.k2{
  background:var(--red)
}


.segment.k3{
  background:var(--green)
}


.x-labels{

  grid-column:3;

  grid-row:2;

  display:flex;

  gap:18px;

  min-width:max-content;

  padding:
    2px 18px 0;

}


.x-label-item{

  min-width:64px;

  text-align:center;

  font-size:10px;

  font-weight:700;

}


.x-axis-title{

  grid-column:3;

  grid-row:3;

  color:#475467;

  text-align:center;

  font-size:12px;

  font-weight:800;

}


.advisor-table td:nth-child(n+2){
  text-align:right
}


.advisor-table tr.total td{

  background:
    var(--yellow);

  font-weight:900;

}


/* ========================================================
   ORDEN MANUAL DE COLUMNAS EN REPORTES
   ======================================================== */

.report-draggable-th{
  cursor:grab;
  user-select:none;
  position:relative;
  transition:background .15s ease, opacity .15s ease, box-shadow .15s ease;
}

.report-draggable-th:active{
  cursor:grabbing;
}

.report-draggable-th::after{
  content:"⋮⋮";
  display:inline-block;
  margin-left:6px;
  color:#98a2b3;
  font-size:10px;
  letter-spacing:-2px;
  vertical-align:middle;
}

.report-draggable-th.dragging{
  opacity:.45;
}

.report-draggable-th.drag-over{
  background:#fff7cc;
  box-shadow:inset 3px 0 0 #d6a800;
}

.matrix td.report-text-cell,
.advisor-table td.report-text-cell{
  text-align:left;
  font-weight:800;
}

.matrix td.report-money-cell,
.advisor-table td.report-money-cell{
  text-align:right;
}

.report-column-hint{
  margin-top:5px;
  color:#667085;
  font-size:11px;
  font-weight:650;
}


.loading{

  opacity:.55;

  pointer-events:none;

}


@media(max-width:1100px){

  header{

    flex-direction:column;

    align-items:flex-start;

  }


  .cards{

    grid-template-columns:
      repeat(
        2,
        1fr
      );

  }


  .filters{

    grid-template-columns:
      repeat(
        2,
        1fr
      );

  }

}


@media(max-width:650px){

  .container{

    padding:
      14px 10px
      30px;

  }


  header{

    padding:
      12px 14px;

  }


  .cards{

    grid-template-columns:
      repeat(
        2,
        minmax(
          0,
          1fr
        )
      );

  }


  .card{

    padding:11px;

  }


  .card .value{

    font-size:20px;

  }


  .filters{

    grid-template-columns:
      1fr;

  }


  .filters .wide{
    grid-column:auto
  }


  .matrix{

    font-size:8.5px;

  }


  .matrix th,
  .matrix td{

    padding:
      4px 2px;

  }

}

</style>

</head>


<body>


<header>

  <div class="brand">

    <div class="logo">
      SD
    </div>

    <div>

      <h1>
        SUPER DUPAZA ONLINE
      </h1>

      <small>
        ZasConnect + BO / VT en la nube
      </small>

    </div>

  </div>


  <button
    class="primary"
    id="btnRefresh"
    onclick="refreshView()"
  >
    ↻ ACTUALIZAR VISTA
  </button>

</header>


<div class="container">


  <div
    id="notice"
    class="notice"
  >
  </div>


  <div class="statusbar">

    <span>

      <span
        id="sheetDot"
        class="dot"
      ></span>

      <b>
        BO / VT:
      </b>

      <span id="sheetStatus">
        revisando...
      </span>

    </span>


    <span>

      <span
        id="zasDot"
        class="dot"
      ></span>

      <b>
        ZasConnect D1:
      </b>

      <span id="zasStatus">
        revisando...
      </span>

    </span>


    <span>

      <b>
        Último cambio BO / VT:
      </b>

      <span id="lastImport">
        —
      </span>

    </span>


    <span>

      <b>
        D1 sincronizado:
      </b>

      <span id="workerUpdated">
        —
      </span>

    </span>


    <span>

      <span
        id="autoSyncStatus"
      >
        Sincronización: revisando...
      </span>

    </span>

  </div>


  <div class="tabs">

    <button
      class="tab active"
      data-view="notLoaded"
      onclick="switchView('notLoaded')"
    >
      Órdenes no cargadas
    </button>


    <button
      class="tab"
      data-view="manualSap"
      onclick="switchView('manualSap')"
    >
      Control SAP
    </button>


    <button
      class="tab"
      data-view="summary"
      onclick="switchView('summary')"
    >
      Resumen BO / VT
    </button>


    <button
      class="tab"
      data-view="advisor"
      onclick="switchView('advisor')"
    >
      Resumen por asesor
    </button>

  </div>


  <!-- ===================================================== -->
  <!-- ÓRDENES NO CARGADAS -->
  <!-- ===================================================== -->


  <section
    id="notLoaded"
    class="view active"
  >

    <div class="cards">

      <div class="card">

        <div class="label">
          Pedidos no cargados
        </div>

        <div
          class="value"
          id="kpiOrders"
        >
          0
        </div>

        <div class="sub">
          Tracking únicos
        </div>

      </div>


      <div class="card">

        <div class="label">
          Líneas
        </div>

        <div
          class="value"
          id="kpiLines"
        >
          0
        </div>

        <div class="sub">
          Artículos
        </div>

      </div>


      <div class="card">

        <div class="label">
          Total pedidos
        </div>

        <div
          class="value"
          id="kpiTotal"
        >
          Q0.00
        </div>

        <div class="sub">
          Una vez por Tracking
        </div>

      </div>


      <div class="card">

        <div class="label">
          Total líneas
        </div>

        <div
          class="value"
          id="kpiTotalLines"
        >
          Q0.00
        </div>

        <div class="sub">
          Suma Total línea
        </div>

      </div>

    </div>


    <div class="panel">

      <div class="panel-title">

        <div>

          <h2>
            Órdenes no cargadas
          </h2>

          <div class="small muted">

            Tracking de ZasConnect que no aparece cargado
            en BO/VT. Las ventas PDV se excluyen de esta comparación.

          </div>

        </div>


        <div
          style="display:flex;gap:8px;flex-wrap:wrap"
        >

          <button
            class="light"
            onclick="openFieldConfig()"
          >
            ⚙ CAMPOS
          </button>


          <button
            class="light"
            onclick="openNameNormalization()"
          >
            ⚙ NOMBRES
          </button>


          <button
            class="light"
            onclick="openManualApiImport()"
          >
            ⬆ IMPORTAR API EXCEL
          </button>


          <button
            id="notLoadedFilterToggle"
            class="light filter-toggle"
            onclick="toggleFilterPanel('notLoadedFilterPanel','notLoadedFilterToggle')"
          >
            ☰ FILTROS
          </button>


          <button
            class="light"
            onclick="exportNotLoadedExcel()"
          >
            ⬇ EXCEL
          </button>

        </div>

      </div>


      <div
        id="notLoadedFilterPanel"
        class="report-filter-panel"
      >

      <div class="filters">

        <input
          class="wide"
          id="searchOrders"
          placeholder="Buscar tracking, cliente, vendedor, NIT..."
          oninput="renderNotLoaded()"
        >


        <select
          id="filterSeller"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todos los vendedores
          </option>

        </select>


        <select
          id="filterDept"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todos los departamentos
          </option>

        </select>


        <select
          id="filterMunicipio"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todos los municipios
          </option>

        </select>


        <select
          id="filterCanal"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todos los canales
          </option>

        </select>


        <select
          id="filterPayment"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todas las formas de pago
          </option>

        </select>


        <select
          id="filterBrand"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todas las marcas
          </option>

        </select>


        <select
          id="filterWarehouse"
          onchange="renderNotLoaded()"
        >

          <option value="">
            Todas las bodegas
          </option>

        </select>


        <input
          id="filterDate"
          type="date"
          onchange="renderNotLoaded()"
        >

      </div>

      </div>


      <div
        id="visibleCounter"
        class="small muted"
        style="margin-bottom:8px"
      >
      </div>


      <div class="table-wrap">

        <table>

          <thead>

            <tr id="ordersHead">
            </tr>

          </thead>

          <tbody id="ordersBody">
          </tbody>

        </table>

      </div>

    </div>

  </section>


  <!-- ===================================================== -->
  <!-- CONTROL SAP -->
  <!-- ===================================================== -->


  <section
    id="manualSap"
    class="view"
  >

    <div class="cards">

      <div class="card">

        <div class="label">
          C en SAP
        </div>

        <div
          class="value"
          id="sapActiveCount"
        >
          0
        </div>

        <div class="sub">
          Pendientes de resolver
        </div>

      </div>


      <div class="card">

        <div class="label">
          C Resuelto
        </div>

        <div
          class="value"
          id="sapResolvedCount"
        >
          0
        </div>

        <div class="sub">
          Se consideran cargados
        </div>

      </div>

    </div>


    <div class="panel">

      <div class="panel-title">

        <div>

          <h2>
            C en SAP
          </h2>

          <div class="small muted">
            Órdenes confirmadas en SAP que ya no aparecen físicamente en BO/VT.
          </div>

        </div>

      </div>


      <div class="table-wrap">

        <table>

          <thead>

            <tr>
              <th>Tracking</th>
              <th>Fecha pedido</th>
              <th>Cliente</th>
              <th>Vendedor</th>
              <th>Total</th>
              <th>Marcado C en SAP</th>
              <th>Acción</th>
            </tr>

          </thead>

          <tbody id="sapActiveBody">
          </tbody>

        </table>

      </div>

    </div>


    <div class="panel">

      <div class="panel-title">

        <div>

          <h2>
            C Resuelto
          </h2>

          <div class="small muted">
            Historial. Estas órdenes se consideran cargadas aunque no estén físicamente en BO/VT.
          </div>

        </div>

      </div>


      <div class="table-wrap">

        <table>

          <thead>

            <tr>
              <th>Tracking</th>
              <th>Fecha pedido</th>
              <th>Cliente</th>
              <th>Vendedor</th>
              <th>Total</th>
              <th>C en SAP</th>
              <th>Resuelto</th>
              <th>Estado</th>
            </tr>

          </thead>

          <tbody id="sapResolvedBody">
          </tbody>

        </table>

      </div>

    </div>

  </section>


  <!-- ===================================================== -->
  <!-- RESUMEN -->
  <!-- ===================================================== -->


  <section
    id="summary"
    class="view"
  >

    <div class="cards">

      <div class="card">

        <div class="label">
          ZasConnect
        </div>

        <div
          class="value"
          id="sumKpiZas"
        >
          Q0.00
        </div>

        <div class="sub">
          Monto principal
        </div>

      </div>


      <div class="card">

        <div class="label">
          PDV agregado BO/VT
        </div>

        <div
          class="value"
          id="sumKpiPdv"
        >
          Q0.00
        </div>

        <div class="sub">
          Solo si no existe en API
        </div>

      </div>


      <div class="card">

        <div class="label">
          Total
        </div>

        <div
          class="value"
          id="sumKpiTotal"
        >
          Q0.00
        </div>

      </div>


      <div class="card">

        <div class="label">
          Pedidos
        </div>

        <div
          class="value"
          id="sumKpiOrders"
        >
          0
        </div>

      </div>


      <div class="card">

        <div class="label">
          Meta período
        </div>

        <div
          class="value"
          id="sumKpiTarget"
        >
          Q0.00
        </div>

        <div class="sub">
          Suma de metas prorrateadas
        </div>

      </div>


      <div class="card">

        <div class="label">
          Tendencia mes
        </div>

        <div
          class="value"
          id="sumKpiTrend"
        >
          Q0.00
        </div>

        <div class="sub">
          Solo para el mes actual
        </div>

      </div>

    </div>


    <div class="panel">

      <div class="panel-title">

        <div>

          <h2>
            Resumen BO / VT
          </h2>

          <div class="small muted">

            ZasConnect es la fuente principal.
            BO/VT aporta fecha y clasificación para PDV.

          </div>

          <div
            id="sumRangeStatus"
            class="range-status"
          >
            Rango: sin consultar
          </div>

          <div class="report-column-hint">
            ↔ Arrastra los encabezados de la tabla para ordenar las columnas. El orden se guarda automáticamente.
          </div>

        </div>


        <div style="display:flex;gap:6px;flex-wrap:wrap">

          <button
            class="light"
            onclick="openReportConfig()"
          >
            ⚙ GRUPOS
          </button>


          <button
            id="summaryFilterToggle"
            class="light filter-toggle"
            onclick="toggleFilterPanel('summaryFilterPanel','summaryFilterToggle')"
          >
            ☰ FILTROS
          </button>


          <button
            class="light"
            onclick="resetReportColumnOrder('summary')"
            title="Volver al orden original de las columnas"
          >
            ↺ ORDEN
          </button>


          <button
            class="light"
            onclick="exportSummaryExcel()"
          >
            ⬇ EXCEL
          </button>

        </div>

      </div>


      <div class="report-primary-controls">

        <label>
          <span>Desde</span>
          <input
            id="sumDateFrom"
            type="date"
          >
        </label>

        <label>
          <span>Hasta</span>
          <input
            id="sumDateTo"
            type="date"
          >
        </label>

        <button
          class="light"
          onclick="loadSummary()"
        >
          Consultar
        </button>

      </div>


      <div
        id="summaryFilterPanel"
        class="report-filter-panel"
      >

      <div class="filters">

        <select
          id="sumGroup"
        >

          <option value="">
            Todos los grupos
          </option>

        </select>


        <select
          id="sumAdvisor"
        >

          <option value="">
            Todos los asesores
          </option>

        </select>


        <select
          id="sumType"
        >

          <option value="">
            Todos los segmentos
          </option>

        </select>


        <select
          id="sumSource"
        >

          <option value="">
            Todas las fuentes
          </option>

        </select>


      </div>


      <div class="filter-panel-actions">
        <button class="light" onclick="setQuickRange('sum','today')">Hoy</button>
        <button class="light" onclick="setQuickRange('sum','7d')">7 días</button>
        <button class="light" onclick="setQuickRange('sum','month')">Este mes</button>
        <button class="light" onclick="setQuickRange('sum','prev')">Mes anterior</button>
        <button class="light" onclick="setQuickRange('sum','3m')">3 meses</button>
      </div>

      </div>


      <div class="report-grid">


        <div class="matrix-wrap">

          <table class="matrix">

            <thead>

              <tr id="summaryHeaderRow">

                <th>
                  GRUPO
                </th>

                <th>
                  DIGITAL
                </th>

                <th>
                  PETAPA
                </th>

                <th>
                  XENACOJ
                </th>

                <th>
                  TOTAL
                </th>

                <th title="Suma de metas mensuales de los vendedores del grupo, prorrateadas por los días del rango">
                  META
                </th>

                <th title="Total del grupo / días seleccionados × días del mes. Solo funciona dentro del mes actual.">
                  TENDENCIA
                </th>

              </tr>

            </thead>

            <tbody id="summaryBody">
            </tbody>

          </table>

        </div>


        <div class="chart-card">

          <div
            class="strong"
            style="text-align:center"
          >
            Participación diaria por grupo
          </div>


          <div class="chart-legend">

            <span>
              <i class="legend-dot legend-k1"></i>
              <span id="legendK1">K1</span>
            </span>

            <span>
              <i class="legend-dot legend-k2"></i>
              <span id="legendK2">K2</span>
            </span>

            <span>
              <i class="legend-dot legend-k3"></i>
              <span id="legendK3">K3</span>
            </span>

          </div>


          <div class="chart-scroll">

            <div class="chart-layout">

              <div class="y-axis-title">
                Participación (%)
              </div>


              <div class="y-ticks">

                <span>100</span>
                <span>75</span>
                <span>50</span>
                <span>25</span>
                <span>0</span>

              </div>


              <div class="plot-area">

                <div
                  id="stackChart"
                  class="stack-chart"
                >
                </div>

              </div>


              <div
                id="xLabels"
                class="x-labels"
              >
              </div>


              <div class="x-axis-title">
                Día
              </div>

            </div>

          </div>

        </div>

      </div>

    </div>

  </section>


  <!-- ===================================================== -->
  <!-- RESUMEN POR ASESOR -->
  <!-- ===================================================== -->


  <section
    id="advisor"
    class="view"
  >

    <div class="cards">

      <div class="card">

        <div class="label">
          Asesores
        </div>

        <div
          class="value"
          id="advKpiAdvisors"
        >
          0
        </div>

      </div>


      <div class="card">

        <div class="label">
          Total
        </div>

        <div
          class="value"
          id="advKpiTotal"
        >
          Q0.00
        </div>

      </div>


      <div class="card">

        <div class="label">
          Digital
        </div>

        <div
          class="value"
          id="advKpiDigital"
        >
          Q0.00
        </div>

      </div>


      <div class="card">

        <div class="label">
          Piso
        </div>

        <div
          class="value"
          id="advKpiPiso"
        >
          Q0.00
        </div>

        <div class="sub">
          Petapa + Xenacoj
        </div>

      </div>


      <div class="card">

        <div class="label">
          Meta período
        </div>

        <div
          class="value"
          id="advKpiTarget"
        >
          Q0.00
        </div>

        <div class="sub">
          Prorrateada por días seleccionados
        </div>

      </div>


      <div class="card">

        <div class="label">
          Tendencia mes
        </div>

        <div
          class="value"
          id="advKpiTrend"
        >
          Q0.00
        </div>

        <div class="sub">
          Solo para el mes actual
        </div>

      </div>

    </div>


    <div class="panel">

      <div class="panel-title">

        <div>

          <h2>
            Resumen por asesor
          </h2>

          <div class="small muted">

            Monto y asesor vienen de ZasConnect.
            BO/VT clasifica únicamente las ventas PDV.

          </div>

          <div
            id="advRangeStatus"
            class="range-status"
          >
            Rango: sin consultar
          </div>

          <div class="report-column-hint">
            ↔ Arrastra los encabezados de la tabla para ordenar las columnas. El orden se guarda automáticamente.
          </div>

        </div>


        <div style="display:flex;gap:6px;flex-wrap:wrap">

          <button
            id="advisorFilterToggle"
            class="light filter-toggle"
            onclick="toggleFilterPanel('advisorFilterPanel','advisorFilterToggle')"
          >
            ☰ FILTROS
          </button>

          <button
            class="light"
            onclick="resetReportColumnOrder('advisor')"
            title="Volver al orden original de las columnas"
          >
            ↺ ORDEN
          </button>


          <button
            class="light"
            onclick="exportAdvisorExcel()"
          >
            ⬇ EXCEL
          </button>

        </div>

      </div>


      <div class="report-primary-controls">

        <label>
          <span>Desde</span>
          <input
            id="advDateFrom"
            type="date"
          >
        </label>

        <label>
          <span>Hasta</span>
          <input
            id="advDateTo"
            type="date"
          >
        </label>

        <button
          class="light"
          onclick="loadAdvisor()"
        >
          Consultar
        </button>

      </div>


      <div
        id="advisorFilterPanel"
        class="report-filter-panel"
      >

      <div class="filters">

        <input
          class="wide"
          id="advSearch"
          placeholder="Buscar asesor..."
          oninput="renderAdvisor()"
        >


      </div>


      <div class="filter-panel-actions">
        <button class="light" onclick="setQuickRange('adv','today')">Hoy</button>
        <button class="light" onclick="setQuickRange('adv','7d')">7 días</button>
        <button class="light" onclick="setQuickRange('adv','month')">Este mes</button>
        <button class="light" onclick="setQuickRange('adv','prev')">Mes anterior</button>
        <button class="light" onclick="setQuickRange('adv','3m')">3 meses</button>
      </div>

      </div>


      <div class="table-wrap">

        <table class="advisor-table">

          <thead>

            <tr id="advisorHeaderRow">

              <th>
                ASESOR
              </th>

              <th>
                MONTO
              </th>

              <th>
                DIGITAL
              </th>

              <th>
                PETAPA
              </th>

              <th>
                XENACOJ
              </th>

              <th title="Meta mensual prorrateada por los días del rango seleccionado">
                META
              </th>

              <th title="Monto del rango / días seleccionados × días del mes. Solo funciona dentro del mes actual.">
                TENDENCIA
              </th>

            </tr>

          </thead>


          <tbody id="advisorBody">
          </tbody>

        </table>

      </div>

    </div>

  </section>


</div>


<div
  id="reportConfigModal"
  class="field-modal"
  onclick="closeReportConfigFromBackdrop(event)"
>

  <div
    class="field-modal-card report-config-card"
    onclick="event.stopPropagation()"
  >

    <div class="field-modal-head">

      <div>

        <h3>
          Configuración visual de grupos
        </h3>

        <div class="small muted">
          K1, K2 y K3 permanecen como códigos internos.
          Aquí solo cambias el nombre que se muestra en Resumen.
        </div>

      </div>


      <button
        class="light"
        onclick="closeReportConfig()"
      >
        ✕
      </button>

    </div>


    <div class="report-label-grid">

      <label>

        <span>
          K1
        </span>

        <input
          id="reportLabelK1"
          maxlength="80"
          placeholder="K1"
        >

      </label>


      <label>

        <span>
          K2
        </span>

        <input
          id="reportLabelK2"
          maxlength="80"
          placeholder="K2"
        >

      </label>


      <label>

        <span>
          K3
        </span>

        <input
          id="reportLabelK3"
          maxlength="80"
          placeholder="K3"
        >

      </label>

    </div>


    <div
      class="name-rule-help"
      style="margin-top:14px"
    >

      <strong>
        Clasificación de vendedores:
      </strong>

      se administra en
      <strong>⚙ NOMBRES</strong>.

      Todo vendedor sin segmento manual se considera
      <strong>DIGITAL</strong> si viene del API.

      Si una orden aparece como
      <strong>PDV en BO/VT</strong>,
      el sistema la cambia automáticamente a
      <strong>PETAPA o XENACOJ</strong>
      y se considera tienda física.

    </div>


    <div class="field-modal-actions">

      <button
        class="light"
        onclick="closeReportConfig()"
      >
        CERRAR
      </button>


      <button
        class="primary"
        onclick="saveReportGroupLabels()"
      >
        GUARDAR NOMBRES
      </button>

    </div>

  </div>

</div>


<div
  id="fieldConfigModal"
  class="field-modal"
  onclick="closeFieldConfigFromBackdrop(event)"
>

  <div
    class="field-modal-card"
    onclick="event.stopPropagation()"
  >

    <div class="field-modal-head">

      <div>
        <h3>Campos de Órdenes no cargadas</h3>
        <div class="small muted">
          Activa o desactiva columnas. La selección queda guardada en este navegador.
        </div>
      </div>

      <button
        class="light"
        onclick="closeFieldConfig()"
      >
        ✕
      </button>

    </div>


    <div
      id="fieldConfigList"
      class="field-config-list"
    >
    </div>


    <div class="field-modal-actions">

      <button
        class="light"
        onclick="selectEssentialFields()"
      >
        ESENCIALES
      </button>

      <button
        class="light"
        onclick="selectAllFields()"
      >
        TODOS
      </button>

      <button
        class="light"
        onclick="clearFieldSelection()"
      >
        NINGUNO
      </button>

      <button
        class="primary"
        onclick="saveFieldConfig()"
      >
        GUARDAR
      </button>

    </div>

  </div>

</div>


<div
  id="nameNormalizationModal"
  class="field-modal"
  onclick="closeNameNormalizationFromBackdrop(event)"
>

  <div
    class="field-modal-card name-normalization-card"
    onclick="event.stopPropagation()"
  >

    <div class="field-modal-head">

      <div>

        <h3>
          Normalización de nombres
        </h3>

        <div class="small muted">
          Define el nombre final, sus variantes y la clasificación del vendedor.
          La normalización de nombre afecta Órdenes no cargadas; K y Segmento alimentan los resúmenes.
        </div>

      </div>


      <div style="display:flex;gap:8px;flex-wrap:wrap">

        <button
          class="light"
          onclick="openSellerTargets()"
        >
          💰 METAS MENSUALES
        </button>

        <button
          class="light"
          onclick="closeNameNormalization()"
        >
          ✕
        </button>

      </div>

    </div>


    <datalist id="rawSellerNames">
    </datalist>


    <div class="name-rule-help">

      <strong>Ejemplo:</strong>
      KAREN PAOLA SOLÍS · K2 · DIGITAL.
      Si la misma orden aparece como PDV en BO/VT,
      PETAPA/XENACOJ gana automáticamente sobre DIGITAL.
      <br><br>
      <strong>Más variantes:</strong>
      cada fila admite hasta 5 variantes. Usa “+ Variantes” para crear
      otra fila del mismo vendedor con 5 espacios adicionales.
      También puedes usar “Nuevo vendedor manual” para agregar un vendedor
      que todavía no exista en el API.

    </div>


    <div class="name-rules-table-wrap">

      <table class="name-rules-table">

        <thead>

          <tr>
            <th>Nombre final</th>
            <th>K</th>
            <th>Segmento</th>
            <th>Opción 1</th>
            <th>Opción 2</th>
            <th>Opción 3</th>
            <th>Opción 4</th>
            <th>Opción 5</th>
            <th>Acciones</th>
          </tr>

        </thead>


        <tbody id="nameRulesBody">
        </tbody>

      </table>

    </div>


    <div class="field-modal-actions">

      <button
        class="primary"
        onclick="addManualSellerRow()"
      >
        ＋ NUEVO VENDEDOR MANUAL
      </button>


      <button
        class="light"
        onclick="addNameNormalizationRow()"
      >
        ＋ NUEVA FILA DE NORMALIZACIÓN
      </button>


      <button
        class="light"
        onclick="closeNameNormalization()"
      >
        CERRAR
      </button>

    </div>

  </div>

</div>


<div
  id="manualApiImportModal"
  class="field-modal"
  onclick="closeManualApiImportFromBackdrop(event)"
>

  <div
    class="field-modal-card manual-api-import-card"
    onclick="event.stopPropagation()"
  >

    <div class="field-modal-head">

      <div>
        <h3>Importar órdenes faltantes como API</h3>
        <div class="small muted">
          Carga un Excel con el formato de Pedidos SAP. Tracking pedido identifica una orden;
          Código artículo, Descripción y Cantidad se conservan como líneas de esa misma orden.
        </div>
      </div>

      <button
        class="light"
        onclick="closeManualApiImport()"
      >
        ✕
      </button>

    </div>


    <div class="name-rule-help">
      <strong>Seguro contra duplicados:</strong>
      si el Tracking pedido ya existe realmente en ZasConnect/D1, el importador lo omite.
      Si es una orden previamente importada manualmente, permite actualizarla.
      El Total pedido se contabiliza una sola vez aunque existan varias líneas de producto.
    </div>


    <div class="manual-api-import-drop">
      <input
        id="manualApiExcelFile"
        type="file"
        accept=".xlsx"
        onchange="readManualApiExcel(this.files && this.files[0])"
      >
      <div class="small muted" style="margin-top:7px">
        Se espera una hoja llamada <strong>Pedidos SAP</strong>; si no existe se utilizará la primera hoja.
      </div>
    </div>


    <div
      id="manualApiImportKpis"
      class="manual-api-import-kpis"
      style="display:none"
    >
      <div class="manual-api-import-kpi"><span>Líneas</span><b id="manualApiKpiLines">0</b></div>
      <div class="manual-api-import-kpi"><span>Órdenes</span><b id="manualApiKpiOrders">0</b></div>
      <div class="manual-api-import-kpi"><span>Total único</span><b id="manualApiKpiTotal">Q0.00</b></div>
      <div class="manual-api-import-kpi"><span>Desde</span><b id="manualApiKpiFrom">—</b></div>
      <div class="manual-api-import-kpi"><span>Hasta</span><b id="manualApiKpiTo">—</b></div>
    </div>


    <div
      id="manualApiImportPreview"
      class="manual-api-import-preview"
      style="display:none"
    ></div>


    <div
      id="manualApiImportStatus"
      class="manual-api-import-status"
    >
      Selecciona un archivo .xlsx para validarlo antes de escribir en D1.
    </div>


    <div class="field-modal-actions">
      <button
        class="light"
        onclick="closeManualApiImport()"
      >
        CERRAR
      </button>

      <button
        id="manualApiImportBtn"
        class="primary"
        onclick="importManualApiToD1()"
        disabled
      >
        IMPORTAR A D1
      </button>
    </div>

  </div>

</div>


<div
  id="sellerTargetsModal"
  class="field-modal"
  onclick="closeSellerTargetsFromBackdrop(event)"
>

  <div
    class="field-modal-card seller-target-card"
    onclick="event.stopPropagation()"
  >

    <div class="field-modal-head">

      <div>
        <h3>Metas mensuales por vendedor</h3>
        <div class="small muted">
          Define una meta distinta para cada mes. En Resumen por asesor se prorratea por los días del rango consultado.
        </div>
      </div>

      <button
        class="light"
        onclick="closeSellerTargets()"
      >
        ✕
      </button>

    </div>


    <div class="seller-target-toolbar">

      <input
        id="sellerTargetMonth"
        type="month"
        onchange="loadSellerTargets()"
      >

      <input
        id="sellerTargetSearch"
        placeholder="Buscar vendedor..."
        oninput="renderSellerTargets()"
      >

    </div>


    <div class="seller-target-table-wrap">

      <table class="seller-target-table">

        <thead>
          <tr>
            <th>VENDEDOR</th>
            <th>META DEL MES</th>
          </tr>
        </thead>

        <tbody id="sellerTargetsBody">
        </tbody>

      </table>

    </div>


    <div class="field-modal-actions">

      <button
        class="light"
        onclick="closeSellerTargets()"
      >
        CERRAR
      </button>

      <button
        class="primary"
        onclick="saveSellerTargets()"
      >
        GUARDAR METAS
      </button>

    </div>

  </div>

</div>


<div
  id="cellTooltip"
  class="cell-tooltip"
></div>


<div
  id="copyToast"
  class="copy-toast"
>
  Copiado
</div>


<script src="https://cdn.jsdelivr.net/npm/exceljs@4.4.0/dist/exceljs.min.js"></script>

<script>

const money =
  new Intl.NumberFormat(
    "es-GT",
    {
      style:
        "currency",

      currency:
        "GTQ",

      minimumFractionDigits:
        2
    }
  );


const integer =
  new Intl.NumberFormat(
    "es-GT",
    {
      maximumFractionDigits:
        0
    }
  );


let orderColumns = [];

let orderRows = [];

let visibleOrderRows = [];

let summaryRows = [];

let advisorRows = [];

let reportOptionsLoaded = false;

let summaryHasExecuted = false;
let advisorHasExecuted = false;
let summaryQueryDirty = false;
let advisorQueryDirty = false;

let sellerTargetRows = [];


let manualApiImportRows = [];
let manualApiImportSummary = null;
let manualApiImportFileName = "";

const PENDING_FIELDS_STORAGE =
  "superDupaza.pendingFields.v2";

let pendingVisibleFieldKeys =
  null;

let tooltipCell =
  null;

let autoRefreshTimer =
  null;

let currentView =
  "notLoaded";


let reportGroupLabels = {
  K1:
    "K1",
  K2:
    "K2",
  K3:
    "K3"
};


const SUMMARY_COLUMN_STORAGE =
  "superDupaza.summaryColumnOrder.v1";

const ADVISOR_COLUMN_STORAGE =
  "superDupaza.advisorColumnOrder.v1";

const SUMMARY_COLUMN_DEFS = [
  {key:"grupo",label:"GRUPO",width:24,money:false},
  {key:"DIGITAL",label:"DIGITAL",money:true},
  {key:"PETAPA",label:"PETAPA",money:true},
  {key:"XENACOJ",label:"XENACOJ",money:true},
  {key:"TOTAL",label:"TOTAL",money:true},
  {
    key:"META",
    label:"META",
    money:true,
    title:"Suma de metas mensuales de los vendedores del grupo, prorrateadas por los días del rango"
  },
  {
    key:"TENDENCIA",
    label:"TENDENCIA",
    money:true,
    title:"Total del grupo / días seleccionados × días del mes. Solo funciona dentro del mes actual."
  }
];

const ADVISOR_COLUMN_DEFS = [
  {key:"ASESOR",label:"ASESOR",width:34,money:false},
  {key:"MONTO",label:"MONTO",money:true},
  {key:"DIGITAL",label:"DIGITAL",money:true},
  {key:"PETAPA",label:"PETAPA",money:true},
  {key:"XENACOJ",label:"XENACOJ",money:true},
  {
    key:"META",
    label:"META",
    money:true,
    title:"Meta mensual prorrateada por los días del rango seleccionado"
  },
  {
    key:"TENDENCIA",
    label:"TENDENCIA",
    money:true,
    title:"Monto del rango / días seleccionados × días del mes. Solo funciona dentro del mes actual."
  }
];

function readReportColumnOrder(
  storageKey,
  defs
) {

  const defaults =
    defs.map(
      column =>
        column.key
    );

  try {

    const saved =
      JSON.parse(
        localStorage.getItem(
          storageKey
        ) ||
        "[]"
      );

    if (
      Array.isArray(saved) &&
      saved.length === defaults.length &&
      defaults.every(
        key =>
          saved.includes(key)
      )
    ) {

      return saved;

    }

  } catch (error) {

    console.warn(
      "No se pudo leer el orden de columnas:",
      error
    );

  }

  return defaults;

}

let summaryColumnOrder =
  readReportColumnOrder(
    SUMMARY_COLUMN_STORAGE,
    SUMMARY_COLUMN_DEFS
  );

let advisorColumnOrder =
  readReportColumnOrder(
    ADVISOR_COLUMN_STORAGE,
    ADVISOR_COLUMN_DEFS
  );

let reportColumnDrag = null;


// ============================================================
// UTILIDADES
// ============================================================

function esc(value) {

  if (
    value === null ||
    value === undefined
  ) {

    return "";

  }


  return String(value)
    .replace(
      /[&<>"']/g,
      char =>
        ({
          "&":"&amp;",
          "<":"&lt;",
          ">":"&gt;",
          '"':"&quot;",
          "'":"&#039;"
        })[char]
    );

}



function showNotice(
  message,
  type = "ok"
) {

  const el =
    document.getElementById(
      "notice"
    );


  if (!message) {

    el.className =
      "notice";

    el.textContent =
      "";

    return;

  }


  el.className =
    "notice show " +
    type;


  el.textContent =
    message;

}



function setBusy(
  busy
) {

  document.body.classList.toggle(
    "loading",
    busy
  );


  document.getElementById(
    "btnRefresh"
  )
    .disabled =
    busy;

}



function formatDateTime(
  value
) {

  if (!value) {
    return "—";
  }


  const date =
    new Date(
      value
    );


  if (
    Number.isNaN(
      date.getTime()
    )
  ) {

    return String(
      value
    );

  }


  return new Intl.DateTimeFormat(
    "es-GT",
    {
      timeZone:
        "America/Guatemala",

      dateStyle:
        "short",

      timeStyle:
        "medium"
    }
  )
    .format(
      date
    );

}



function fillSelect(
  id,
  values,
  label
) {

  const el =
    document.getElementById(
      id
    );


  const previous =
    el.value;


  el.innerHTML =
    '<option value="">' +
    esc(label) +
    "</option>" +
    (
      values ||
      []
    )
      .map(
        value =>
          '<option value="' +
          esc(value) +
          '">' +
          esc(value) +
          "</option>"
      )
      .join(
        ""
      );


  if (
    [
      ...el.options
    ]
      .some(
        option =>
          option.value ===
          previous
      )
  ) {

    el.value =
      previous;

  }

}




function fillGroupSelect(
  id,
  values,
  label
) {

  const el =
    document.getElementById(
      id
    );


  const previous =
    el.value;


  el.innerHTML =
    '<option value="">' +
    esc(
      label
    ) +
    "</option>" +
    (
      values ||
      []
    )
      .map(
        item => {

          const value =
            typeof item ===
            "string"
              ? item
              : item.value;


          const text =
            typeof item ===
            "string"
              ? (
                  reportGroupLabels[
                    item
                  ] ||
                  item
                )
              : item.label;


          return (
            '<option value="' +
            esc(
              value
            ) +
            '">' +
            esc(
              text
            ) +
            "</option>"
          );

        }
      )
      .join(
        ""
      );


  if (
    [
      ...el.options
    ]
      .some(
        option =>
          option.value ===
          previous
      )
  ) {

    el.value =
      previous;

  }

}



function updateReportVisualLabels() {

  const mapping = {
    legendK1:
      reportGroupLabels.K1 ||
      "K1",

    legendK2:
      reportGroupLabels.K2 ||
      "K2",

    legendK3:
      reportGroupLabels.K3 ||
      "K3"
  };


  for (
    const [
      id,
      value
    ]
    of Object.entries(
      mapping
    )
  ) {

    const el =
      document.getElementById(
        id
      );


    if (el) {

      el.textContent =
        value;

    }

  }

}



function toggleFilterPanel(
  panelId,
  buttonId
) {

  const panel =
    document.getElementById(
      panelId
    );


  const button =
    document.getElementById(
      buttonId
    );


  if (!panel) {
    return;
  }


  panel.classList.toggle(
    "show"
  );


  if (button) {

    button.classList.toggle(
      "active",
      panel.classList.contains(
        "show"
      )
    );

  }

}



function wireDatePicker(
  id,
  reportPrefix,
  isEnd
) {

  const input =
    document.getElementById(
      id
    );


  if (!input) {
    return;
  }


  input.addEventListener(
    "click",
    () => {

      try {

        if (
          typeof input.showPicker ===
          "function"
        ) {

          input.showPicker();

        }

      } catch {
        // El navegador abrirá su selector nativo.
      }

    }
  );


  input.addEventListener(
    "change",
    async () => {

      if (
        reportPrefix ===
        "sum"
      ) {

        if (
          summaryHasExecuted
        ) {

          summaryQueryDirty =
            true;

          return;

        }


        if (
          isEnd &&
          document.getElementById(
            "sumDateFrom"
          ).value &&
          document.getElementById(
            "sumDateTo"
          ).value
        ) {

          try {
            await loadSummary();
          } catch (error) {
            showNotice(
              error.message,
              "bad"
            );
          }

        }

        return;

      }


      if (
        advisorHasExecuted
      ) {

        advisorQueryDirty =
          true;

        return;

      }


      if (
        isEnd &&
        document.getElementById(
          "advDateFrom"
        ).value &&
        document.getElementById(
          "advDateTo"
        ).value
      ) {

        try {
          await loadAdvisor();
        } catch (error) {
          showNotice(
            error.message,
            "bad"
          );
        }

      }

    }
  );

}



function setupReportDateUx() {

  wireDatePicker(
    "sumDateFrom",
    "sum",
    false
  );

  wireDatePicker(
    "sumDateTo",
    "sum",
    true
  );

  wireDatePicker(
    "advDateFrom",
    "adv",
    false
  );

  wireDatePicker(
    "advDateTo",
    "adv",
    true
  );


  for (
    const pickerId
    of [
      "filterDate",
      "sellerTargetMonth"
    ]
  ) {

    const picker =
      document.getElementById(
        pickerId
      );


    picker?.addEventListener(
      "click",
      () => {

        try {
          if (
            typeof picker.showPicker ===
            "function"
          ) {
            picker.showPicker();
          }
        } catch {
          // Selector nativo del navegador.
        }

      }
    );

  }


  for (
    const id
    of [
      "sumGroup",
      "sumAdvisor",
      "sumType",
      "sumSource"
    ]
  ) {

    document.getElementById(
      id
    )?.addEventListener(
      "change",
      () => {
        if (
          summaryHasExecuted
        ) {
          summaryQueryDirty =
            true;
        }
      }
    );

  }

}



function closeReportConfig() {

  document
    .getElementById(
      "reportConfigModal"
    )
    .classList.remove(
      "show"
    );

}



function closeReportConfigFromBackdrop(
  event
) {

  if (
    event.target.id ===
    "reportConfigModal"
  ) {

    closeReportConfig();

  }

}



async function openReportConfig() {

  document
    .getElementById(
      "reportConfigModal"
    )
    .classList.add(
      "show"
    );


  try {

    const response =
      await fetch(
        "/api/report-config",
        {
          cache:
            "no-store"
        }
      );


    const data =
      await response.json();


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudo cargar la configuración."
      );

    }


    reportGroupLabels = {
      ...reportGroupLabels,
      ...(
        data.labels ||
        {}
      )
    };


    document
      .getElementById(
        "reportLabelK1"
      )
      .value =
      reportGroupLabels.K1 ||
      "K1";


    document
      .getElementById(
        "reportLabelK2"
      )
      .value =
      reportGroupLabels.K2 ||
      "K2";


    document
      .getElementById(
        "reportLabelK3"
      )
      .value =
      reportGroupLabels.K3 ||
      "K3";


    updateReportVisualLabels();


  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



async function saveReportGroupLabels() {

  const labels = {
    K1:
      document
        .getElementById(
          "reportLabelK1"
        )
        .value
        .trim(),

    K2:
      document
        .getElementById(
          "reportLabelK2"
        )
        .value
        .trim(),

    K3:
      document
        .getElementById(
          "reportLabelK3"
        )
        .value
        .trim()
  };


  const token =
    getSapControlToken();


  if (!token) {
    return;
  }


  try {

    const response =
      await fetch(
        "/api/report-config/labels",
        {
          method:
            "POST",

          headers: {
            "Content-Type":
              "application/json",

            "X-Super-Dupaza-Admin":
              token
          },

          body:
            JSON.stringify({
              labels
            })
        }
      );


    const data =
      await response.json();


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudieron guardar los nombres."
      );

    }


    reportGroupLabels = {
      ...reportGroupLabels,
      ...(
        data.labels ||
        labels
      )
    };


    updateReportVisualLabels();


    reportOptionsLoaded =
      false;


    await loadReportOptions();


    if (
      document
        .getElementById(
          "sumDateFrom"
        )
        .value &&
      document
        .getElementById(
          "sumDateTo"
        )
        .value
    ) {

      await loadSummary();

    }


    showNotice(
      "Nombres visuales guardados.",
      "ok"
    );


  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



function queryString(
  params
) {

  const q =
    new URLSearchParams();


  Object.entries(
    params
  )
    .forEach(
      (
        [
          key,
          value
        ]
      ) => {

        if (value) {

          q.set(
            key,
            value
          );

        }

      }
    );


  return q.toString();

}



function switchView(
  id
) {

  currentView =
    id;


  document
    .querySelectorAll(
      ".view"
    )
    .forEach(
      el =>
        el.classList.toggle(
          "active",
          el.id === id
        )
    );


  document
    .querySelectorAll(
      ".tab"
    )
    .forEach(
      el =>
        el.classList.toggle(
          "active",
          el.dataset.view === id
        )
    );


  if (
    id ===
    "manualSap"
  ) {

    loadManualSap()
      .catch(
        error =>
          showNotice(
            String(
              error?.message ||
              error
            ),
            "bad"
          )
      );

  }

}





function pendingDefaultEssentialKeys() {

  return [
    "customerTrackingCode",
    "taxId",
    "customerName",
    "primaryPhone",
    "sellerName",
    "shippingMunicipality",
    "shippingDepartment",
    "orderDate",
    "sku",
    "description",
    "quantity",
    "trackingCode",
    "warehouse",
    "orderTotal",
    "custom_payment_method"
  ];

}


function readPendingFieldKeys() {

  if (
    pendingVisibleFieldKeys
  ) {

    return pendingVisibleFieldKeys;

  }


  try {

    const stored =
      JSON.parse(
        localStorage.getItem(
          PENDING_FIELDS_STORAGE
        ) ||
        "null"
      );


    if (
      Array.isArray(
        stored
      ) &&
      stored.length
    ) {

      pendingVisibleFieldKeys =
        stored;


      return stored;

    }

  } catch {
    // Usar todos los campos disponibles como compatibilidad inicial.
  }


  pendingVisibleFieldKeys =
    orderColumns.map(
      column =>
        column.key
    );


  return pendingVisibleFieldKeys;

}


function visiblePendingColumns() {

  const selected =
    new Set(
      readPendingFieldKeys()
    );


  const result =
    orderColumns.filter(
      column =>
        selected.has(
          column.key
        )
    );


  return result.length
    ? result
    : orderColumns.slice(
        0,
        1
      );

}


function openFieldConfig() {

  const selected =
    new Set(
      readPendingFieldKeys()
    );


  document
    .getElementById(
      "fieldConfigList"
    )
    .innerHTML =
    orderColumns
      .map(
        column =>
          '<label class="field-option">' +
          '<input type="checkbox" data-field-key="' +
          esc(
            column.key
          ) +
          '" ' +
          (
            selected.has(
              column.key
            )
              ? "checked"
              : ""
          ) +
          '>' +
          '<span>' +
          esc(
            column.label
          ) +
          '</span>' +
          '</label>'
      )
      .join(
        ""
      );


  document
    .getElementById(
      "fieldConfigModal"
    )
    .classList.add(
      "show"
    );

}


function closeFieldConfig() {

  document
    .getElementById(
      "fieldConfigModal"
    )
    .classList.remove(
      "show"
    );

}


function closeFieldConfigFromBackdrop(
  event
) {

  if (
    event.target.id ===
    "fieldConfigModal"
  ) {

    closeFieldConfig();

  }

}


function setFieldCheckboxes(
  keys
) {

  const selected =
    new Set(
      keys
    );


  document
    .querySelectorAll(
      "#fieldConfigList input[data-field-key]"
    )
    .forEach(
      input => {

        input.checked =
          selected.has(
            input.dataset.fieldKey
          );

      }
    );

}


function selectEssentialFields() {

  setFieldCheckboxes(
    pendingDefaultEssentialKeys()
  );

}


function selectAllFields() {

  setFieldCheckboxes(
    orderColumns.map(
      column =>
        column.key
    )
  );

}


function clearFieldSelection() {

  setFieldCheckboxes(
    []
  );

}


function saveFieldConfig() {

  const selected =
    [
      ...document.querySelectorAll(
        "#fieldConfigList input[data-field-key]:checked"
      )
    ]
      .map(
        input =>
          input.dataset.fieldKey
      );


  if (
    !selected.length
  ) {

    showNotice(
      "Selecciona al menos un campo.",
      "bad"
    );

    return;

  }


  pendingVisibleFieldKeys =
    selected;


  localStorage.setItem(
    PENDING_FIELDS_STORAGE,
    JSON.stringify(
      selected
    )
  );


  closeFieldConfig();

  renderNotLoaded();

}


function applyPendingCellSize() {

  const table =
    document.querySelector(
      "#notLoaded table"
    );


  if (!table) {
    return;
  }


  const columns =
    visiblePendingColumns()
      .length +
    1;


  const available =
    table.parentElement?.clientWidth ||
    table.clientWidth ||
    window.innerWidth;


  const size =
    Math.max(
      28,
      Math.min(
        58,
        Math.floor(
          available /
          Math.max(
            columns,
            1
          )
        )
      )
    );


  table.style.setProperty(
    "--pending-cell-size",
    size +
    "px"
  );

}


function hideCellTooltip() {

  tooltipCell =
    null;


  const tooltip =
    document.getElementById(
      "cellTooltip"
    );


  tooltip.classList.remove(
    "show"
  );

}


function positionCellTooltip(
  event
) {

  const tooltip =
    document.getElementById(
      "cellTooltip"
    );


  if (
    !tooltip.classList.contains(
      "show"
    )
  ) {
    return;
  }


  const margin = 12;
  const offset = 14;

  let left =
    event.clientX +
    offset;

  let top =
    event.clientY +
    offset;


  const width =
    tooltip.offsetWidth;

  const height =
    tooltip.offsetHeight;


  if (
    left +
    width +
    margin >
    window.innerWidth
  ) {

    left =
      Math.max(
        margin,
        event.clientX -
        width -
        offset
      );

  }


  if (
    top +
    height +
    margin >
    window.innerHeight
  ) {

    top =
      Math.max(
        margin,
        event.clientY -
        height -
        offset
      );

  }


  tooltip.style.left =
    left +
    "px";


  tooltip.style.top =
    top +
    "px";

}


function attachPendingCellInteractions() {

  document
    .querySelectorAll(
      "#ordersBody td[data-copy]"
    )
    .forEach(
      cell => {

        cell.addEventListener(
          "click",
          () => {

            copyText(
              decodeURIComponent(
                cell.dataset.copy ||
                ""
              ),
              cell.dataset.copyMessage
            );

          }
        );


        cell.addEventListener(
          "mouseenter",
          event => {

            tooltipCell =
              cell;


            const tooltip =
              document.getElementById(
                "cellTooltip"
              );


            const fullValue =
              decodeURIComponent(
                cell.dataset.full ||
                ""
              );


            tooltip.textContent =
              fullValue ||
              "(vacío)";


            tooltip.classList.add(
              "show"
            );


            positionCellTooltip(
              event
            );

          }
        );


        cell.addEventListener(
          "mousemove",
          event =>
            positionCellTooltip(
              event
            )
        );


        cell.addEventListener(
          "mouseleave",
          hideCellTooltip
        );

      }
    );

}



let nameRulesRows = [];

let nameRuleTempId = -1;


function closeNameNormalization() {

  document
    .getElementById(
      "nameNormalizationModal"
    )
    .classList.remove(
      "show"
    );

}



function closeNameNormalizationFromBackdrop(
  event
) {

  if (
    event.target.id ===
    "nameNormalizationModal"
  ) {

    closeNameNormalization();

  }

}



function renderRawSellerDatalist(
  values
) {

  document
    .getElementById(
      "rawSellerNames"
    )
    .innerHTML =
    (
      values ||
      []
    )
      .map(
        value =>
          '<option value="' +
          esc(
            value
          ) +
          '"></option>'
      )
      .join(
        ""
      );

}



function currentGuatemalaMonth() {

  const parts =
    new Intl.DateTimeFormat(
      "en-CA",
      {
        timeZone:
          "America/Guatemala",
        year:
          "numeric",
        month:
          "2-digit"
      }
    )
      .formatToParts(
        new Date()
      );


  return (
    parts.find(
      item =>
        item.type ===
        "year"
    )?.value +
    "-" +
    parts.find(
      item =>
        item.type ===
        "month"
    )?.value
  );

}



function closeSellerTargets() {

  document
    .getElementById(
      "sellerTargetsModal"
    )
    .classList.remove(
      "show"
    );

}



function closeSellerTargetsFromBackdrop(
  event
) {

  if (
    event.target.id ===
    "sellerTargetsModal"
  ) {

    closeSellerTargets();

  }

}



async function openSellerTargets() {

  const monthInput =
    document.getElementById(
      "sellerTargetMonth"
    );


  if (
    !monthInput.value
  ) {

    monthInput.value =
      currentGuatemalaMonth();

  }


  document
    .getElementById(
      "sellerTargetsModal"
    )
    .classList.add(
      "show"
    );


  await loadSellerTargets();

}



async function loadSellerTargets() {

  const month =
    document.getElementById(
      "sellerTargetMonth"
    ).value;


  const body =
    document.getElementById(
      "sellerTargetsBody"
    );


  body.innerHTML =
    '<tr><td colspan="2" class="muted">Cargando metas...</td></tr>';


  try {

    const response =
      await fetch(
        "/api/seller-targets?month=" +
        encodeURIComponent(
          month
        ),
        {
          cache:
            "no-store"
        }
      );


    const data =
      await response.json();


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudieron cargar las metas."
      );

    }


    sellerTargetRows =
      (
        data.rows ||
        []
      )
        .map(
          row => {

            const amount =
              Number(
                row.target ||
                0
              );


            return {
              seller:
                row.seller ||
                "",
              amount,
              originalAmount:
                amount
            };

          }
        );


    renderSellerTargets();

  } catch (error) {

    body.innerHTML =
      '<tr><td colspan="2" class="muted">No se pudieron cargar las metas.</td></tr>';


    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



function renderSellerTargets() {

  const search =
    (
      document.getElementById(
        "sellerTargetSearch"
      ).value ||
      ""
    )
      .trim()
      .toLowerCase();


  const rows =
    sellerTargetRows
      .map(
        (row, index) => ({
          ...row,
          index
        })
      )
      .filter(
        row =>
          !search ||
          String(
            row.seller ||
            ""
          )
            .toLowerCase()
            .includes(
              search
            )
      );


  document.getElementById(
    "sellerTargetsBody"
  ).innerHTML =
    rows.length
      ? rows
          .map(
            row =>
              "<tr>" +
              '<td class="strong">' +
              esc(
                row.seller
              ) +
              "</td>" +
              "<td>" +
              '<input type="number" min="0" step="0.01" data-target-index="' +
              row.index +
              '" value="' +
              esc(
                Number(
                  row.amount ||
                  0
                ) ||
                ""
              ) +
              '" placeholder="0.00" oninput="updateSellerTargetValue(' +
              row.index +
              ',this.value)">' +
              "</td>" +
              "</tr>"
          )
          .join(
            ""
          )
      : '<tr><td colspan="2" class="muted">No hay vendedores que coincidan con la búsqueda.</td></tr>';

}



function updateSellerTargetValue(
  index,
  value
) {

  if (
    !sellerTargetRows[
      index
    ]
  ) {
    return;
  }


  sellerTargetRows[
    index
  ].amount =
    Math.max(
      0,
      Number(
        value ||
        0
      ) ||
      0
    );

}



async function saveSellerTargets() {

  const month =
    document.getElementById(
      "sellerTargetMonth"
    ).value;


  const token =
    getSapControlToken();


  if (!token) {
    return;
  }


  const changedTargets =
    sellerTargetRows
      .filter(
        row =>
          Math.abs(
            Number(
              row.amount ||
              0
            ) -
            Number(
              row.originalAmount ||
              0
            )
          ) >
          0.004
      );


  if (
    !changedTargets.length
  ) {

    showNotice(
      "No hay cambios de metas para guardar.",
      "ok"
    );

    return;

  }


  try {

    const response =
      await fetch(
        "/api/seller-targets/save",
        {
          method:
            "POST",

          headers: {
            "Content-Type":
              "application/json",

            "X-Super-Dupaza-Admin":
              token
          },

          body:
            JSON.stringify({
              month,
              targets:
                changedTargets.map(
                  row => ({
                    seller:
                      row.seller,
                    amount:
                      Number(
                        row.amount ||
                        0
                      )
                  })
                )
            })
        }
      );


    const data =
      await response.json();


    if (
      response.status ===
      401
    ) {

      sessionStorage.removeItem(
        "superDupazaSapToken"
      );

    }


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudieron guardar las metas."
      );

    }


    showNotice(
      "Metas de " +
      month +
      " guardadas.",
      "ok"
    );


    await loadSellerTargets();


    if (
      advisorHasExecuted &&
      !advisorQueryDirty
    ) {
      await loadAdvisor();
    }

  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



function nameRuleInput(
  rowId,
  field,
  value,
  placeholder
) {

  return (
    '<input ' +
    'data-name-rule-id="' +
    esc(
      rowId
    ) +
    '" ' +
    'data-name-rule-field="' +
    esc(
      field
    ) +
    '" ' +
    (
      field ===
      "canonical"
        ? ""
        : 'list="rawSellerNames" '
    ) +
    'value="' +
    esc(
      value ||
      ""
    ) +
    '" ' +
    'placeholder="' +
    esc(
      placeholder
    ) +
    '">' 
  );

}




function nameRuleSelect(
  rowId,
  field,
  value,
  options
) {

  return (
    '<select ' +
    'data-name-rule-id="' +
    esc(
      rowId
    ) +
    '" ' +
    'data-name-rule-field="' +
    esc(
      field
    ) +
    '">' +

    options
      .map(
        option =>
          '<option value="' +
          esc(
            option[0]
          ) +
          '" ' +
          (
            option[0] ===
            (
              value ||
              ""
            )
              ? "selected"
              : ""
          ) +
          '>' +
          esc(
            option[1]
          ) +
          '</option>'
      )
      .join(
        ""
      ) +

    '</select>'
  );

}



function renderNameRules() {

  const body =
    document.getElementById(
      "nameRulesBody"
    );


  body.innerHTML =
    nameRulesRows
      .map(
        row => {

          const aliases =
            [
              ...(
                row.aliases ||
                []
              ),
              "",
              "",
              "",
              "",
              ""
            ]
              .slice(
                0,
                5
              );


          return (
            '<tr class="' +
            (
              Number(
                row.id
              ) <
              0
                ? "name-rule-new"
                : ""
            ) +
            '">' +

            '<td>' +
            nameRuleInput(
              row.id,
              "canonical",
              row.canonical,
              "Nombre final"
            ) +
            '</td>' +

            '<td>' +
            nameRuleSelect(
              row.id,
              "kLevel",
              row.kLevel,
              [
                ["","AUTO"],
                ["K1","K1"],
                ["K2","K2"],
                ["K3","K3"]
              ]
            ) +
            '</td>' +

            '<td>' +
            nameRuleSelect(
              row.id,
              "segment",
              row.segment,
              [
                ["","AUTO"],
                ["DIGITAL","DIGITAL"],
                ["PETAPA","PETAPA"],
                ["XENACOJ","XENACOJ"]
              ]
            ) +
            '</td>' +

            aliases
              .map(
                (
                  alias,
                  index
                ) =>
                  '<td>' +
                  nameRuleInput(
                    row.id,
                    "alias" +
                    (
                      index +
                      1
                    ),
                    alias,
                    "Opción " +
                    (
                      index +
                      1
                    )
                  ) +
                  '</td>'
              )
              .join(
                ""
              ) +

            '<td><div class="name-rule-actions">' +

            '<button class="primary" onclick="saveNameNormalizationRow(' +
            Number(
              row.id
            ) +
            ')">Guardar</button>' +

            '<button class="light" onclick="addMoreAliasesForRow(' +
            Number(
              row.id
            ) +
            ')">+ Variantes</button>' +

            '<button class="light" onclick="deleteNameNormalizationRow(' +
            Number(
              row.id
            ) +
            ')">Eliminar</button>' +

            '</div></td>' +

            '</tr>'
          );

        }
      )
      .join(
        ""
      );


  if (
    !nameRulesRows.length
  ) {

    body.innerHTML =
      '<tr><td colspan="9" class="muted">No hay normalizaciones. Usa “Nuevo vendedor manual” para crear la primera.</td></tr>';

  }

}



async function openNameNormalization() {

  document
    .getElementById(
      "nameNormalizationModal"
    )
    .classList.add(
      "show"
    );


  document
    .getElementById(
      "nameRulesBody"
    )
    .innerHTML =
    '<tr><td colspan="9" class="muted">Cargando normalizaciones...</td></tr>';


  try {

    const response =
      await fetch(
        "/api/name-normalization/list",
        {
          cache:
            "no-store"
        }
      );


    const data =
      await response.json();


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudieron cargar las normalizaciones."
      );

    }


    nameRulesRows =
      (
        data.rules ||
        []
      )
        .map(
          row => ({
            id:
              Number(
                row.id
              ),
            canonical:
              row.canonical ||
              "",
            aliases:
              row.aliases ||
              [],

            kLevel:
              row.kLevel ||
              null,

            segment:
              row.segment ||
              null
          })
        );


    renderRawSellerDatalist(
      data.rawSellers ||
      []
    );


    renderNameRules();


  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );


    document
      .getElementById(
        "nameRulesBody"
      )
      .innerHTML =
      '<tr><td colspan="9" class="muted">No se pudo cargar la configuración.</td></tr>';

  }

}




function addManualSellerRow() {

  nameRulesRows.unshift({
    id:
      nameRuleTempId--,

    canonical:
      "",

    aliases:
      [],

    kLevel:
      null,

    segment:
      null,

    _manual:
      true
  });


  renderNameRules();


  const firstInput =
    document.querySelector(
      '#nameRulesBody input[data-name-rule-field="canonical"]'
    );


  firstInput?.focus();


  showNotice(
    "Nueva fila manual creada. Escribe el vendedor, K, segmento y variantes; luego pulsa Guardar.",
    "ok"
  );

}



function addMoreAliasesForRow(
  rowId
) {

  const source =
    nameRulesRows.find(
      row =>
        Number(
          row.id
        ) ===
        Number(
          rowId
        )
    );


  if (!source) {

    showNotice(
      "No se encontró la normalización seleccionada.",
      "bad"
    );

    return;

  }


  const canonical =
    String(
      source.canonical ||
      ""
    )
      .trim();


  if (!canonical) {

    showNotice(
      "Guarda primero el Nombre final antes de agregar más variantes.",
      "bad"
    );

    return;

  }


  const newId =
    nameRuleTempId--;


  const currentIndex =
    nameRulesRows.findIndex(
      row =>
        Number(
          row.id
        ) ===
        Number(
          rowId
        )
    );


  const clone = {
    id:
      newId,

    canonical,

    aliases:
      [],

    kLevel:
      source.kLevel ||
      null,

    segment:
      source.segment ||
      null,

    _manual:
      true
  };


  if (
    currentIndex >=
    0
  ) {

    nameRulesRows.splice(
      currentIndex + 1,
      0,
      clone
    );

  } else {

    nameRulesRows.unshift(
      clone
    );

  }


  renderNameRules();


  const firstAlias =
    document.querySelector(
      '#nameRulesBody input[data-name-rule-id="' +
      newId +
      '"][data-name-rule-field="alias1"]'
    );


  firstAlias?.focus();


  showNotice(
    "Se agregaron 5 espacios adicionales para variantes de " +
    canonical +
    ".",
    "ok"
  );

}



function addNameNormalizationRow() {

  nameRulesRows.unshift({
    id:
      nameRuleTempId--,

    canonical:
      "",

    aliases:
      [],

    kLevel:
      null,

    segment:
      null,

    _manual:
      true
  });


  renderNameRules();


  const firstInput =
    document.querySelector(
      '#nameRulesBody input[data-name-rule-field="canonical"]'
    );


  firstInput?.focus();

}



function readNameRuleFromRow(
  rowId
) {

  const inputs =
    [
      ...document.querySelectorAll(
        '#nameRulesBody [data-name-rule-id="' +
        rowId +
        '"]'
      )
    ];


  const values =
    Object.fromEntries(
      inputs.map(
        input => [
          input.dataset.nameRuleField,
          input.value.trim()
        ]
      )
    );


  return {
    id:
      Number(
        rowId
      ) >
      0
        ? Number(
            rowId
          )
        : null,

    canonical:
      values.canonical ||
      "",

    aliases: [
      values.alias1,
      values.alias2,
      values.alias3,
      values.alias4,
      values.alias5
    ]
      .filter(Boolean),

    kLevel:
      values.kLevel ||
      null,

    segment:
      values.segment ||
      null
  };

}



async function postNameNormalization(
  path,
  body
) {

  const token =
    getSapControlToken();


  if (!token) {

    throw new Error(
      "Se canceló la operación."
    );

  }


  const response =
    await fetch(
      path,
      {
        method:
          "POST",

        headers: {
          "Content-Type":
            "application/json",

          "X-Super-Dupaza-Admin":
            token
        },

        body:
          JSON.stringify(
            body
          )
      }
    );


  const data =
    await response.json();


  if (
    response.status ===
    401
  ) {

    sessionStorage.removeItem(
      "superDupazaSapToken"
    );

  }


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "No se pudo guardar la normalización."
    );

  }


  return data;

}



async function saveNameNormalizationRow(
  rowId
) {

  const rule =
    readNameRuleFromRow(
      rowId
    );


  if (
    !rule.canonical
  ) {

    showNotice(
      "Escribe el Nombre final.",
      "bad"
    );

    return;

  }


  try {

    await postNameNormalization(
      "/api/name-normalization/save",
      rule
    );


    showNotice(
      "Normalización guardada.",
      "ok"
    );


    await Promise.all([
      openNameNormalization(),
      loadNotLoaded()
    ]);


  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



async function deleteNameNormalizationRow(
  rowId
) {

  if (
    Number(
      rowId
    ) <
    0
  ) {

    nameRulesRows =
      nameRulesRows.filter(
        row =>
          Number(
            row.id
          ) !==
          Number(
            rowId
          )
      );


    renderNameRules();


    return;

  }


  if (
    !confirm(
      "¿Eliminar esta normalización?"
    )
  ) {
    return;
  }


  try {

    await postNameNormalization(
      "/api/name-normalization/delete",
      {
        id:
          Number(
            rowId
          )
      }
    );


    showNotice(
      "Normalización eliminada.",
      "ok"
    );


    await Promise.all([
      openNameNormalization(),
      loadNotLoaded()
    ]);


  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}




function closeManualApiImport() {

  document
    .getElementById(
      "manualApiImportModal"
    )
    .classList.remove(
      "show"
    );

}


function closeManualApiImportFromBackdrop(
  event
) {

  if (
    event.target.id ===
    "manualApiImportModal"
  ) {

    closeManualApiImport();

  }

}


async function openManualApiImport() {

  document
    .getElementById(
      "manualApiImportModal"
    )
    .classList.add(
      "show"
    );


  const status =
    document.getElementById(
      "manualApiImportStatus"
    );


  if (
    !manualApiImportRows.length
  ) {

    status.textContent =
      "Selecciona un archivo .xlsx para validarlo antes de escribir en D1.";

  }

}


function manualApiHeaderKey(
  value
) {

  return String(
    value ??
    ""
  )
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(
      /[\u0300-\u036f]/g,
      ""
    )
    .replace(
      /\s+/g,
      " "
    );

}


function manualApiScalar(
  value
) {

  if (
    value ===
    null ||
    value ===
    undefined
  ) {
    return null;
  }


  if (
    value instanceof Date
  ) {

    const y =
      value.getFullYear();

    const m =
      String(
        value.getMonth() +
        1
      )
        .padStart(
          2,
          "0"
        );

    const d =
      String(
        value.getDate()
      )
        .padStart(
          2,
          "0"
        );

    return (
      y +
      "-" +
      m +
      "-" +
      d
    );

  }


  if (
    typeof value ===
      "object"
  ) {

    if (
      Object.prototype.hasOwnProperty.call(
        value,
        "result"
      )
    ) {

      return manualApiScalar(
        value.result
      );

    }


    if (
      Array.isArray(
        value.richText
      )
    ) {

      return value.richText
        .map(
          part =>
            part.text ||
            ""
        )
        .join("");

    }


    if (
      value.text !==
      undefined
    ) {

      return String(
        value.text
      );

    }

  }


  return value;

}


function manualApiDate(
  value
) {

  const scalar =
    manualApiScalar(
      value
    );


  if (
    scalar ===
      null ||
    scalar ===
      ""
  ) {
    return null;
  }


  if (
    typeof scalar ===
      "number" &&
    Number.isFinite(
      scalar
    )
  ) {

    const utc =
      new Date(
        Date.UTC(
          1899,
          11,
          30
        ) +
        scalar *
        86400000
      );

    return utc
      .toISOString()
      .slice(
        0,
        10
      );

  }


  const text =
    String(
      scalar
    )
      .trim();


  const iso =
    text.match(
      /^(\d{4})-(\d{2})-(\d{2})/
    );


  if (iso) {
    return iso[0];
  }


  const slash =
    text.match(
      /^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/
    );


  if (slash) {

    return (
      slash[3] +
      "-" +
      String(
        slash[2]
      ).padStart(
        2,
        "0"
      ) +
      "-" +
      String(
        slash[1]
      ).padStart(
        2,
        "0"
      )
    );

  }


  return text;

}


function manualApiNumber(
  value
) {

  const scalar =
    manualApiScalar(
      value
    );


  if (
    typeof scalar ===
      "number"
  ) {

    return Number.isFinite(
      scalar
    )
      ? scalar
      : 0;

  }


  const parsed =
    Number(
      String(
        scalar ??
        ""
      )
        .replace(
          /Q/gi,
          ""
        )
        .replace(
          /,/g,
          ""
        )
        .trim()
    );


  return Number.isFinite(
    parsed
  )
    ? parsed
    : 0;

}


function manualApiFindColumn(
  headers,
  names,
  occurrence = 1
) {

  const wanted =
    (
      Array.isArray(
        names
      )
        ? names
        : [names]
    )
      .map(
        manualApiHeaderKey
      );


  let seen =
    0;


  for (
    let i = 0;
    i < headers.length;
    i++
  ) {

    if (
      wanted.includes(
        manualApiHeaderKey(
          headers[i]
        )
      )
    ) {

      seen++;


      if (
        seen ===
        occurrence
      ) {

        return i +
          1;

      }

    }

  }


  return 0;

}


async function readManualApiExcel(
  file
) {

  const status =
    document.getElementById(
      "manualApiImportStatus"
    );

  const button =
    document.getElementById(
      "manualApiImportBtn"
    );


  manualApiImportRows = [];
  manualApiImportSummary = null;
  manualApiImportFileName = "";
  button.disabled =
    true;


  if (!file) {

    status.textContent =
      "Selecciona un archivo .xlsx.";

    return;

  }


  if (
    !window.ExcelJS
  ) {

    status.textContent =
      "No se cargó ExcelJS en el navegador.";

    return;

  }


  status.textContent =
    "Leyendo y validando " +
    file.name +
    "...";


  try {

    const workbook =
      new ExcelJS.Workbook();


    await workbook.xlsx.load(
      await file.arrayBuffer()
    );


    const worksheet =
      workbook.getWorksheet(
        "Pedidos SAP"
      ) ||
      workbook.worksheets[0];


    if (!worksheet) {

      throw new Error(
        "El Excel no contiene hojas."
      );

    }


    const headerRow =
      worksheet.getRow(
        1
      );


    const headers = [];


    for (
      let c = 1;
      c <= worksheet.columnCount;
      c++
    ) {

      headers.push(
        manualApiScalar(
          headerRow.getCell(
            c
          ).value
        ) ||
        ""
      );

    }


    const col = {
      customerTrackingCode:
        manualApiFindColumn(
          headers,
          [
            "Tracking cliente",
            "customerTrackingCode"
          ]
        ),
      taxId:
        manualApiFindColumn(
          headers,
          [
            "NIT",
            "taxId"
          ]
        ),
      customerName:
        manualApiFindColumn(
          headers,
          [
            "Cliente",
            "customerName"
          ]
        ),
      primaryPhone:
        manualApiFindColumn(
          headers,
          [
            "Teléfono principal",
            "Telefono principal",
            "primaryPhone"
          ]
        ),
      secondaryPhone:
        manualApiFindColumn(
          headers,
          [
            "Teléfono secundario",
            "Telefono secundario",
            "secondaryPhone"
          ]
        ),
      sellerName:
        manualApiFindColumn(
          headers,
          [
            "Vendedor",
            "sellerName"
          ]
        ),
      shippingMunicipality:
        manualApiFindColumn(
          headers,
          [
            "Municipio",
            "shippingMunicipality"
          ]
        ),
      shippingReference:
        manualApiFindColumn(
          headers,
          [
            "Referencia de entrega",
            "shippingReference"
          ]
        ),
      custom_rcf:
        manualApiFindColumn(
          headers,
          [
            "RCF",
            "custom_rcf"
          ]
        ),
      salesChannel:
        manualApiFindColumn(
          headers,
          [
            "Canal de venta",
            "salesChannel"
          ]
        ),
      shippingDepartment:
        manualApiFindColumn(
          headers,
          [
            "Departamento",
            "shippingDepartment"
          ]
        ),
      orderDate:
        manualApiFindColumn(
          headers,
          [
            "Fecha pedido",
            "orderDate"
          ]
        ),
      sku:
        manualApiFindColumn(
          headers,
          [
            "Código artículo",
            "Codigo articulo",
            "sku"
          ]
        ),
      description:
        manualApiFindColumn(
          headers,
          [
            "Descripción",
            "Descripcion",
            "description"
          ]
        ),
      quantity:
        manualApiFindColumn(
          headers,
          [
            "Cantidad",
            "quantity"
          ]
        ),
      brand:
        manualApiFindColumn(
          headers,
          [
            "Marca",
            "brand"
          ],
          1
        ),
      locationCode:
        manualApiFindColumn(
          headers,
          [
            "Código de ubicación",
            "Codigo de ubicacion",
            "locationCode"
          ]
        ),
      ringCode:
        manualApiFindColumn(
          headers,
          [
            "Código de anillo",
            "Codigo de anillo",
            "ringCode"
          ]
        ),
      comments:
        manualApiFindColumn(
          headers,
          [
            "Comentarios",
            "comments"
          ]
        ),
      trackingCode:
        manualApiFindColumn(
          headers,
          [
            "Tracking pedido",
            "trackingCode"
          ]
        ),
      warehouse:
        manualApiFindColumn(
          headers,
          [
            "Bodega",
            "warehouse"
          ],
          1
        ),
      orderTotal:
        manualApiFindColumn(
          headers,
          [
            "Total pedido",
            "orderTotal"
          ]
        ),
      unitPrice:
        manualApiFindColumn(
          headers,
          [
            "Precio unitario",
            "unitPrice"
          ]
        ),
      lineNumber:
        manualApiFindColumn(
          headers,
          [
            "Línea",
            "Linea",
            "lineNumber"
          ]
        ),
      shippingAvenue:
        manualApiFindColumn(
          headers,
          [
            "Avenida o calle",
            "shippingAvenue"
          ]
        ),
      shippingHouseNumber:
        manualApiFindColumn(
          headers,
          [
            "Casa o lote",
            "shippingHouseNumber"
          ]
        ),
      shippingZone:
        manualApiFindColumn(
          headers,
          [
            "Zona",
            "shippingZone"
          ]
        ),
      lineTotal:
        manualApiFindColumn(
          headers,
          [
            "Total línea",
            "Total linea",
            "lineTotal"
          ]
        ),
      custom_bodega:
        manualApiFindColumn(
          headers,
          [
            "Bodega",
            "custom_bodega"
          ],
          2
        ),
      custom_canal:
        manualApiFindColumn(
          headers,
          [
            "Canal",
            "custom_canal"
          ]
        ),
      custom_payment_method:
        manualApiFindColumn(
          headers,
          [
            "Forma de pago",
            "custom_payment_method"
          ]
        ),
      custom_marca:
        manualApiFindColumn(
          headers,
          [
            "Marca",
            "custom_marca"
          ],
          2
        )
    };


    const required = [
      [
        "Tracking pedido",
        col.trackingCode
      ],
      [
        "Fecha pedido",
        col.orderDate
      ],
      [
        "Vendedor",
        col.sellerName
      ],
      [
        "Cliente",
        col.customerName
      ],
      [
        "Total pedido",
        col.orderTotal
      ],
      [
        "Código artículo",
        col.sku
      ],
      [
        "Descripción",
        col.description
      ],
      [
        "Cantidad",
        col.quantity
      ]
    ];


    const missing =
      required
        .filter(
          item =>
            !item[1]
        )
        .map(
          item =>
            item[0]
        );


    if (
      missing.length
    ) {

      throw new Error(
        "Faltan columnas requeridas: " +
        missing.join(", ")
      );

    }


    const readCell =
      (
        row,
        index
      ) =>
        index
          ? manualApiScalar(
              row.getCell(
                index
              ).value
            )
          : null;


    const rows = [];
    let blankTracking = 0;


    worksheet.eachRow(
      {
        includeEmpty:
          false
      },
      (
        row,
        rowNumber
      ) => {

        if (
          rowNumber ===
          1
        ) {
          return;
        }


        const tracking =
          String(
            readCell(
              row,
              col.trackingCode
            ) ??
            ""
          )
            .trim();


        const sku =
          String(
            readCell(
              row,
              col.sku
            ) ??
            ""
          )
            .trim();


        const customer =
          String(
            readCell(
              row,
              col.customerName
            ) ??
            ""
          )
            .trim();


        if (
          !tracking &&
          !sku &&
          !customer
        ) {
          return;
        }


        if (!tracking) {

          blankTracking++;
          return;

        }


        rows.push({
          customerTrackingCode:
            readCell(
              row,
              col.customerTrackingCode
            ),
          taxId:
            readCell(
              row,
              col.taxId
            ),
          customerName:
            readCell(
              row,
              col.customerName
            ),
          primaryPhone:
            readCell(
              row,
              col.primaryPhone
            ),
          secondaryPhone:
            readCell(
              row,
              col.secondaryPhone
            ),
          sellerName:
            readCell(
              row,
              col.sellerName
            ),
          shippingMunicipality:
            readCell(
              row,
              col.shippingMunicipality
            ),
          shippingReference:
            readCell(
              row,
              col.shippingReference
            ),
          custom_rcf:
            readCell(
              row,
              col.custom_rcf
            ),
          salesChannel:
            readCell(
              row,
              col.salesChannel
            ),
          shippingDepartment:
            readCell(
              row,
              col.shippingDepartment
            ),
          orderDate:
            manualApiDate(
              readCell(
                row,
                col.orderDate
              )
            ),
          sku:
            readCell(
              row,
              col.sku
            ),
          description:
            readCell(
              row,
              col.description
            ),
          quantity:
            manualApiNumber(
              readCell(
                row,
                col.quantity
              )
            ),
          brand:
            readCell(
              row,
              col.brand
            ),
          locationCode:
            readCell(
              row,
              col.locationCode
            ),
          ringCode:
            readCell(
              row,
              col.ringCode
            ),
          comments:
            readCell(
              row,
              col.comments
            ),
          trackingCode:
            tracking,
          warehouse:
            readCell(
              row,
              col.warehouse
            ),
          orderTotal:
            manualApiNumber(
              readCell(
                row,
                col.orderTotal
              )
            ),
          unitPrice:
            manualApiNumber(
              readCell(
                row,
                col.unitPrice
              )
            ),
          lineNumber:
            readCell(
              row,
              col.lineNumber
            ),
          shippingAvenue:
            readCell(
              row,
              col.shippingAvenue
            ),
          shippingHouseNumber:
            readCell(
              row,
              col.shippingHouseNumber
            ),
          shippingZone:
            readCell(
              row,
              col.shippingZone
            ),
          lineTotal:
            manualApiNumber(
              readCell(
                row,
                col.lineTotal
              )
            ),
          custom_bodega:
            readCell(
              row,
              col.custom_bodega
            ),
          custom_canal:
            readCell(
              row,
              col.custom_canal
            ),
          custom_payment_method:
            readCell(
              row,
              col.custom_payment_method
            ),
          custom_marca:
            readCell(
              row,
              col.custom_marca
            )
        });

      }
    );


    if (blankTracking) {

      throw new Error(
        "Hay " +
        blankTracking +
        " fila(s) con datos pero sin Tracking pedido."
      );

    }


    if (!rows.length) {

      throw new Error(
        "No se encontraron órdenes para importar."
      );

    }


    const groups =
      new Map();


    for (
      const row
      of rows
    ) {

      const tracking =
        String(
          row.trackingCode
        )
          .trim()
          .toUpperCase();


      row.trackingCode =
        tracking;


      if (
        !groups.has(
          tracking
        )
      ) {

        groups.set(
          tracking,
          []
        );

      }


      groups
        .get(
          tracking
        )
        .push(
          row
        );

    }


    const inconsistencies = [];


    for (
      const [
        tracking,
        items
      ]
      of groups.entries()
    ) {

      const first =
        items[0];


      const firstSeller =
        manualApiHeaderKey(
          first.sellerName
        );

      const firstCustomer =
        manualApiHeaderKey(
          first.customerName
        );

      const firstDate =
        String(
          first.orderDate ||
          ""
        );

      const firstTotal =
        Number(
          first.orderTotal ||
          0
        );


      if (
        items.some(
          item =>
            manualApiHeaderKey(
              item.sellerName
            ) !==
              firstSeller ||
            manualApiHeaderKey(
              item.customerName
            ) !==
              firstCustomer ||
            String(
              item.orderDate ||
              ""
            ) !==
              firstDate ||
            Math.abs(
              Number(
                item.orderTotal ||
                0
              ) -
              firstTotal
            ) >
              0.01
        )
      ) {

        inconsistencies.push(
          tracking
        );

      }

    }


    if (
      inconsistencies.length
    ) {

      throw new Error(
        "Hay Tracking pedido con datos de cabecera distintos entre líneas: " +
        inconsistencies
          .slice(
            0,
            10
          )
          .join(", ")
      );

    }


    const orders =
      [
        ...groups.entries()
      ]
        .map(
          ([
            tracking,
            items
          ]) => ({
            tracking,
            customer:
              items[0].customerName ||
              "",
            seller:
              items[0].sellerName ||
              "",
            date:
              items[0].orderDate ||
              "",
            total:
              Number(
                items[0].orderTotal ||
                0
              ),
            lines:
              items.length
          })
        )
        .sort(
          (
            a,
            b
          ) =>
            String(
              a.date
            ).localeCompare(
              String(
                b.date
              )
            ) ||
            a.tracking.localeCompare(
              b.tracking
            )
        );


    const dates =
      orders
        .map(
          order =>
            order.date
        )
        .filter(Boolean)
        .sort();


    const total =
      orders.reduce(
        (
          sum,
          order
        ) =>
          sum +
          order.total,
        0
      );


    manualApiImportRows =
      rows;

    manualApiImportFileName =
      file.name;

    manualApiImportSummary = {
      lines:
        rows.length,
      orders:
        orders.length,
      total,
      from:
        dates[0] ||
        "—",
      to:
        dates[
          dates.length -
          1
        ] ||
        "—"
    };


    document.getElementById(
      "manualApiKpiLines"
    ).textContent =
      integer.format(
        rows.length
      );

    document.getElementById(
      "manualApiKpiOrders"
    ).textContent =
      integer.format(
        orders.length
      );

    document.getElementById(
      "manualApiKpiTotal"
    ).textContent =
      money.format(
        total
      );

    document.getElementById(
      "manualApiKpiFrom"
    ).textContent =
      manualApiImportSummary.from;

    document.getElementById(
      "manualApiKpiTo"
    ).textContent =
      manualApiImportSummary.to;

    document.getElementById(
      "manualApiImportKpis"
    ).style.display =
      "grid";


    const previewOrders =
      orders.slice(
        0,
        20
      );


    document.getElementById(
      "manualApiImportPreview"
    ).innerHTML =
      '<table><thead><tr>' +
      '<th>Tracking pedido</th>' +
      '<th>Fecha</th>' +
      '<th>Cliente</th>' +
      '<th>Vendedor</th>' +
      '<th>Total</th>' +
      '<th>Líneas</th>' +
      '</tr></thead><tbody>' +
      previewOrders
        .map(
          order =>
            '<tr>' +
            '<td>' +
            esc(
              order.tracking
            ) +
            '</td>' +
            '<td>' +
            esc(
              order.date
            ) +
            '</td>' +
            '<td>' +
            esc(
              order.customer
            ) +
            '</td>' +
            '<td>' +
            esc(
              order.seller
            ) +
            '</td>' +
            '<td>' +
            esc(
              money.format(
                order.total
              )
            ) +
            '</td>' +
            '<td>' +
            esc(
              String(
                order.lines
              )
            ) +
            '</td>' +
            '</tr>'
        )
        .join("") +
      '</tbody></table>';

    document.getElementById(
      "manualApiImportPreview"
    ).style.display =
      "block";


    status.textContent =
      "Validación correcta. " +
      orders.length +
      " órdenes únicas / " +
      rows.length +
      " líneas. Revisa la vista previa y pulsa IMPORTAR A D1.";


    button.disabled =
      false;

  } catch (error) {

    manualApiImportRows = [];
    manualApiImportSummary = null;
    manualApiImportFileName = "";
    button.disabled =
      true;

    document.getElementById(
      "manualApiImportKpis"
    ).style.display =
      "none";

    document.getElementById(
      "manualApiImportPreview"
    ).style.display =
      "none";

    status.textContent =
      "ERROR: " +
      String(
        error?.message ||
        error
      );

  }

}


async function importManualApiToD1() {

  if (
    !manualApiImportRows.length ||
    !manualApiImportSummary
  ) {

    showNotice(
      "Primero selecciona y valida un Excel.",
      "bad"
    );

    return;

  }


  const token =
    getSapControlToken();


  if (!token) {
    return;
  }


  if (
    !confirm(
      "¿Importar " +
      manualApiImportSummary.orders +
      " órdenes / " +
      manualApiImportSummary.lines +
      " líneas a D1? Los tracking que ya existan por API se omitirán."
    )
  ) {
    return;
  }


  const status =
    document.getElementById(
      "manualApiImportStatus"
    );

  const button =
    document.getElementById(
      "manualApiImportBtn"
    );


  button.disabled =
    true;

  status.textContent =
    "Importando a D1 y reconstruyendo reportes...";


  try {

    const response =
      await fetch(
        "/api/manual-api-import",
        {
          method:
            "POST",
          headers: {
            "Content-Type":
              "application/json",
            "X-Super-Dupaza-Admin":
              token
          },
          body:
            JSON.stringify({
              fileName:
                manualApiImportFileName,
              rows:
                manualApiImportRows
            })
        }
      );


    const data =
      await response.json();


    if (
      response.status ===
      401
    ) {

      sessionStorage.removeItem(
        "superDupazaSapToken"
      );

    }


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudo importar el Excel."
      );

    }


    status.textContent =
      "IMPORTACIÓN COMPLETADA\n" +
      "Órdenes del archivo: " +
      data.uniqueOrders +
      "\nImportadas/actualizadas: " +
      data.importedOrders +
      "\nNuevas manuales: " +
      data.newManualOrders +
      "\nActualizadas manuales: " +
      data.updatedManualOrders +
      "\nOmitidas porque ya existen por API: " +
      data.skippedExistingApi +
      "\nLíneas guardadas: " +
      data.importedLines +
      "\nTotal importado: " +
      money.format(
        Number(
          data.totalAmount ||
          0
        )
      );


    showNotice(
      "Excel importado a D1 correctamente.",
      "ok"
    );


    await loadNotLoaded();


    if (
      summaryHasExecuted &&
      !summaryQueryDirty
    ) {
      await loadSummary();
    }


    if (
      advisorHasExecuted &&
      !advisorQueryDirty
    ) {
      await loadAdvisor();
    }

  } catch (error) {

    button.disabled =
      false;

    status.textContent =
      "ERROR: " +
      String(
        error?.message ||
        error
      );

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}


function getSapControlToken() {

  let token =
    sessionStorage.getItem(
      "superDupazaSapToken"
    ) ||
    "";


  if (!token) {

    token =
      prompt(
        "Ingresa la clave administrativa de SUPER DUPAZA:"
      ) ||
      "";


    if (token) {

      sessionStorage.setItem(
        "superDupazaSapToken",
        token
      );

    }

  }


  return token;

}



function formatTimestampGt(
  value
) {

  if (!value) {
    return "";
  }


  try {

    return new Intl.DateTimeFormat(
      "es-GT",
      {
        timeZone:
          "America/Guatemala",

        dateStyle:
          "short",

        timeStyle:
          "short"
      }
    )
      .format(
        new Date(
          value
        )
      );

  } catch {

    return String(
      value
    );

  }

}



async function postSapControl(
  path,
  trackingCode
) {

  const token =
    getSapControlToken();


  if (!token) {

    throw new Error(
      "Se canceló la operación."
    );

  }


  const response =
    await fetch(
      path,
      {
        method:
          "POST",

        headers: {
          "Content-Type":
            "application/json",

          "X-Super-Dupaza-Admin":
            token
        },

        body:
          JSON.stringify({
            trackingCode
          })
      }
    );


  const data =
    await response.json();


  if (
    response.status ===
    401
  ) {

    sessionStorage.removeItem(
      "superDupazaSapToken"
    );

  }


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "No se pudo actualizar Control SAP."
    );

  }


  return data;

}



async function markOrderInSap(
  trackingCode
) {

  if (
    !confirm(
      "¿Confirmar que esta orden está en SAP y moverla al listado Control SAP?"
    )
  ) {
    return;
  }


  try {

    const data =
      await postSapControl(
        "/api/manual-sap/mark",
        trackingCode
      );


    copyToast(
      data.message ||
      "Orden movida a Control SAP."
    );


    await loadNotLoaded();


    if (
      currentView ===
      "manualSap"
    ) {

      await loadManualSap();

    }

  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



async function resolveSapOrder(
  trackingCode
) {

  if (
    !confirm(
      "¿Marcar esta orden como C Resuelto? Se considerará cargada aunque no aparezca físicamente en BO/VT."
    )
  ) {
    return;
  }


  try {

    const data =
      await postSapControl(
        "/api/manual-sap/resolve",
        trackingCode
      );


    copyToast(
      data.message ||
      "Orden resuelta."
    );


    await Promise.all([
      loadManualSap(),
      loadNotLoaded()
    ]);

  } catch (error) {

    showNotice(
      String(
        error?.message ||
        error
      ),
      "bad"
    );

  }

}



async function loadManualSap() {

  const response =
    await fetch(
      "/api/manual-sap/list",
      {
        cache:
          "no-store"
      }
    );


  const data =
    await response.json();


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "No se pudo cargar Control SAP."
    );

  }


  document
    .getElementById(
      "sapActiveCount"
    )
    .textContent =
    integer.format(
      data.counts?.active ||
      0
    );


  document
    .getElementById(
      "sapResolvedCount"
    )
    .textContent =
    integer.format(
      data.counts?.resolved ||
      0
    );


  document
    .getElementById(
      "sapActiveBody"
    )
    .innerHTML =
    (
      data.active ||
      []
    )
      .map(
        row =>
          "<tr>" +
          "<td>" +
          esc(
            row.tracking_code
          ) +
          "</td>" +
          "<td>" +
          esc(
            row.order_date ||
            ""
          ) +
          "</td>" +
          "<td>" +
          esc(
            row.customer_name ||
            ""
          ) +
          "</td>" +
          "<td>" +
          esc(
            row.seller_name ||
            ""
          ) +
          "</td>" +
          "<td>" +
          esc(
            money.format(
              Number(
                row.order_total ||
                0
              )
            )
          ) +
          "</td>" +
          "<td>" +
          esc(
            formatTimestampGt(
              row.marked_sap_at
            )
          ) +
          "</td>" +
          '<td><button class="sap-action-btn" onclick="resolveSapOrder(\'' +
          esc(
            row.tracking_code
          ) +
          '\')">C Resuelto</button></td>' +
          "</tr>"
      )
      .join(
        ""
      );


  if (
    !(
      data.active ||
      []
    )
      .length
  ) {

    document
      .getElementById(
        "sapActiveBody"
      )
      .innerHTML =
      '<tr><td colspan="7" class="muted">No hay órdenes pendientes en Control SAP.</td></tr>';

  }


  document
    .getElementById(
      "sapResolvedBody"
    )
    .innerHTML =
    (
      data.resolved ||
      []
    )
      .map(
        row =>
          "<tr>" +
          "<td>" +
          esc(
            row.tracking_code
          ) +
          "</td>" +
          "<td>" +
          esc(
            row.order_date ||
            ""
          ) +
          "</td>" +
          "<td>" +
          esc(
            row.customer_name ||
            ""
          ) +
          "</td>" +
          "<td>" +
          esc(
            row.seller_name ||
            ""
          ) +
          "</td>" +
          "<td>" +
          esc(
            money.format(
              Number(
                row.order_total ||
                0
              )
            )
          ) +
          "</td>" +
          "<td>" +
          esc(
            formatTimestampGt(
              row.marked_sap_at
            )
          ) +
          "</td>" +
          "<td>" +
          esc(
            formatTimestampGt(
              row.resolved_at
            )
          ) +
          "</td>" +
          '<td><span class="manual-status resolved">C Resuelto</span></td>' +
          "</tr>"
      )
      .join(
        ""
      );


  if (
    !(
      data.resolved ||
      []
    )
      .length
  ) {

    document
      .getElementById(
        "sapResolvedBody"
      )
      .innerHTML =
      '<tr><td colspan="8" class="muted">Todavía no hay órdenes resueltas.</td></tr>';

  }

}



let copyTimer =
  null;


function copyToast(
  text
) {

  const el =
    document.getElementById(
      "copyToast"
    );


  el.textContent =
    text;


  el.classList.add(
    "show"
  );


  clearTimeout(
    copyTimer
  );


  copyTimer =
    setTimeout(
      () =>
        el.classList.remove(
          "show"
        ),
      1200
    );

}



async function copyText(
  value,
  message
) {

  const text =
    String(
      value ??
      ""
    );


  if (!text) {
    return;
  }


  try {

    await navigator.clipboard
      .writeText(
        text
      );

  } catch {

    const ta =
      document.createElement(
        "textarea"
      );


    ta.value =
      text;


    ta.style.position =
      "fixed";


    ta.style.opacity =
      "0";


    document.body.appendChild(
      ta
    );


    ta.select();


    document.execCommand(
      "copy"
    );


    ta.remove();

  }


  copyToast(
    message ||
    "Valor copiado"
  );

}


// ============================================================
// STATUS
// ============================================================

async function loadStatus() {

  try {

    const response =
      await fetch(
        "/api/source/status"
      );


    const data =
      await response.json();


    if (
      !response.ok ||
      !data.ok
    ) {

      throw new Error(
        data.error ||
        "No se pudo consultar estado."
      );

    }


    document
      .getElementById(
        "sheetDot"
      )
      .className =
      "dot ok";


    document
      .getElementById(
        "sheetStatus"
      )
      .textContent =
      "OK";


    document
      .getElementById(
        "zasDot"
      )
      .className =
      "dot ok";


    document
      .getElementById(
        "zasStatus"
      )
      .textContent =
      "OK · " +
      integer.format(
        data.zasConnect?.orders ||
        0
      ) +
      " pedidos";


    document
      .getElementById(
        "lastImport"
      )
      .textContent =
      formatDateTime(
        data.googleSheets?.lastImport
      );




    const autoEl =
      document.getElementById(
        "autoSyncStatus"
      );


    const scheduler =
      data.scheduler ||
      {};


    const cronAge =
      scheduler.ageMinutes;


    if (
      !scheduler.hasRun
    ) {

      autoEl.innerHTML =
        '<span style="color:#b42318;font-weight:800">Cron no ha ejecutado todavía</span>';

    } else if (
      scheduler.lastError
    ) {

      autoEl.innerHTML =
        '<span style="color:#b42318;font-weight:800">Cron con error · revisa Eventos/Logs</span>';

    } else if (
      scheduler.stale
    ) {

      autoEl.innerHTML =
        '<span style="color:#b42318;font-weight:800">Cron atrasado' +
        (
          cronAge ===
          null
            ? ""
            : " · último éxito hace " +
              integer.format(
                cronAge
              ) +
              " min"
        ) +
        '</span>';

    } else {

      autoEl.innerHTML =
        '<span style="color:#067647;font-weight:800">Cron OK · cada 5 min · último éxito hace ' +
        integer.format(
          cronAge ||
          0
        ) +
        ' min</span>';

    }
    document
      .getElementById(
        "workerUpdated"
      )
      .textContent =
      formatDateTime(
        data.generatedAt
      );


  } catch (error) {

    document
      .getElementById(
        "sheetDot"
      )
      .className =
      "dot bad";


    document
      .getElementById(
        "zasDot"
      )
      .className =
      "dot bad";


    showNotice(
      error.message,
      "bad"
    );

  }

}


// ============================================================
// NO CARGADAS
// ============================================================

async function loadNotLoaded() {

  const response =
    await fetch(
      "/api/not-loaded?ts=" +
      Date.now(),
      {
        cache:
          "no-store"
      }
    );


  const data =
    await response.json();


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "Error cargando órdenes no cargadas."
    );

  }


  orderColumns =
    data.columns ||
    [];


  orderRows =
    data.rows ||
    [];


  window.__SUPER_DUPAZA_PENDING_DIAGNOSTICS__ =
    data.diagnostics ||
    {};


  document
    .getElementById(
      "kpiOrders"
    )
    .textContent =
    integer.format(
      data.kpis?.orders ||
      0
    );


  document
    .getElementById(
      "kpiLines"
    )
    .textContent =
    integer.format(
      data.kpis?.lines ||
      0
    );


  document
    .getElementById(
      "kpiTotal"
    )
    .textContent =
    money.format(
      data.kpis?.total ||
      0
    );


  document
    .getElementById(
      "kpiTotalLines"
    )
    .textContent =
    money.format(
      data.kpis?.totalLines ||
      0
    );


  const options =
    data.options ||
    {};


  fillSelect(
    "filterSeller",
    options.sellers ||
    [],
    "Todos los vendedores"
  );


  fillSelect(
    "filterDept",
    options.departments ||
    [],
    "Todos los departamentos"
  );


  fillSelect(
    "filterMunicipio",
    options.municipalities ||
    [],
    "Todos los municipios"
  );


  fillSelect(
    "filterCanal",
    options.channels ||
    [],
    "Todos los canales"
  );


  fillSelect(
    "filterPayment",
    options.payments ||
    [],
    "Todas las formas de pago"
  );


  fillSelect(
    "filterBrand",
    options.brands ||
    [],
    "Todas las marcas"
  );


  fillSelect(
    "filterWarehouse",
    options.warehouses ||
    [],
    "Todas las bodegas"
  );


  renderNotLoaded();

}




function guatemalaTodayIso() {

  const parts =
    new Intl.DateTimeFormat(
      "en-US",
      {
        timeZone:
          "America/Guatemala",

        year:
          "numeric",

        month:
          "2-digit",

        day:
          "2-digit"
      }
    )
      .formatToParts(
        new Date()
      );


  const get =
    type =>
      parts.find(
        part =>
          part.type ===
          type
      )?.value ||
      "";


  return (
    get(
      "year"
    ) +
    "-" +
    get(
      "month"
    ) +
    "-" +
    get(
      "day"
    )
  );

}



function pendingOrderDateIso(
  value
) {

  const text =
    String(
      value ??
      ""
    )
      .trim();


  if (!text) {
    return "";
  }


  const iso =
    text.match(
      /^(\d{4})-(\d{2})-(\d{2})/
    );


  if (iso) {

    return (
      iso[1] +
      "-" +
      iso[2] +
      "-" +
      iso[3]
    );

  }


  const gt =
    text.match(
      /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/
    );


  if (gt) {

    return (
      gt[3] +
      "-" +
      String(
        Number(
          gt[2]
        )
      )
        .padStart(
          2,
          "0"
        ) +
      "-" +
      String(
        Number(
          gt[1]
        )
      )
        .padStart(
          2,
          "0"
        )
    );

  }


  return "";

}



function isPendingOrderFromAnotherDay(
  row,
  todayGt
) {

  const orderDate =
    pendingOrderDateIso(
      row?.orderDate
    );


  return Boolean(
    orderDate &&
    orderDate !==
    todayGt
  );

}




function pendingCellTitle(
  row,
  key
) {

  if (
    key === "sellerName" &&
    row?._sellerOriginal &&
    String(row._sellerOriginal).trim() &&
    String(row._sellerOriginal).trim() !==
      String(row.sellerName ?? "").trim()
  ) {

    return (
      "Nombre real API: " +
      String(row._sellerOriginal).trim()
    );

  }

  return "";

}


function renderNotLoaded() {

  const search =
    (
      document
        .getElementById(
          "searchOrders"
        )
        .value ||
      ""
    )
      .trim()
      .toLowerCase();


  const seller =
    document
      .getElementById(
        "filterSeller"
      )
      .value;


  const department =
    document
      .getElementById(
        "filterDept"
      )
      .value;


  const municipality =
    document
      .getElementById(
        "filterMunicipio"
      )
      .value;


  const channel =
    document
      .getElementById(
        "filterCanal"
      )
      .value;


  const payment =
    document
      .getElementById(
        "filterPayment"
      )
      .value;


  const brand =
    document
      .getElementById(
        "filterBrand"
      )
      .value;


  const warehouse =
    document
      .getElementById(
        "filterWarehouse"
      )
      .value;


  const date =
    document
      .getElementById(
        "filterDate"
      )
      .value;


  const todayGt =
    guatemalaTodayIso();


  const linePositions =
    new Map();


  const rowsByTracking =
    new Map();


  for (
    const row
    of orderRows
  ) {

    const tracking =
      String(
        row.trackingCode ??
        ""
      );


    if (!tracking) {
      continue;
    }


    if (
      !rowsByTracking.has(
        tracking
      )
    ) {

      rowsByTracking.set(
        tracking,
        []
      );

    }


    rowsByTracking
      .get(
        tracking
      )
      .push(
        row
      );

  }


  for (
    const [
      tracking,
      rows
    ]
    of rowsByTracking
  ) {

    rows.forEach(
      (
        row,
        index
      ) => {

        linePositions.set(
          row,
          {
            index:
              index + 1,
            total:
              rows.length,
            tracking
          }
        );

      }
    );

  }


  visibleOrderRows =
    orderRows.filter(
      row => {

        const haystack =
          Object.values(
            row
          )
            .map(
              value =>
                typeof value ===
                  "object"
                  ? ""
                  : String(
                      value ??
                      ""
                    )
                      .toLowerCase()
            )
            .join(
              " "
            );


        if (
          search &&
          !haystack.includes(
            search
          )
        ) {
          return false;
        }


        if (
          seller &&
          String(
            row.sellerName ??
            ""
          ) !==
          seller
        ) {
          return false;
        }


        if (
          department &&
          String(
            row.shippingDepartment ??
            ""
          ) !==
          department
        ) {
          return false;
        }


        if (
          municipality &&
          String(
            row.shippingMunicipality ??
            ""
          ) !==
          municipality
        ) {
          return false;
        }


        const rowChannel =
          String(
            row.custom_canal ??
            row.salesChannel ??
            ""
          );


        if (
          channel &&
          rowChannel !==
          channel
        ) {
          return false;
        }


        if (
          payment &&
          String(
            row.custom_payment_method ??
            ""
          ) !==
          payment
        ) {
          return false;
        }


        const rowBrand =
          String(
            row.custom_marca ??
            row.brand ??
            ""
          );


        if (
          brand &&
          rowBrand !==
          brand
        ) {
          return false;
        }


        const rowWarehouse =
          String(
            row.warehouse ??
            row.custom_bodega ??
            ""
          );


        if (
          warehouse &&
          rowWarehouse !==
          warehouse
        ) {
          return false;
        }


        if (
          date &&
          String(
            row.orderDate ??
            ""
          )
            .slice(
              0,
              10
            ) !==
          date
        ) {
          return false;
        }


        return true;

      }
    );


  const oldOrderTrackings =
    new Set(
      visibleOrderRows
        .filter(
          row =>
            isPendingOrderFromAnotherDay(
              row,
              todayGt
            )
        )
        .map(
          row =>
            String(
              row.trackingCode ??
              ""
            )
        )
        .filter(Boolean)
    );


  document
    .getElementById(
      "visibleCounter"
    )
    .innerHTML =
    "Mostrando " +
    integer.format(
      visibleOrderRows.length
    ) +
    " líneas de " +
    integer.format(
      orderRows.length
    ) +
    (
      oldOrderTrackings.size
        ? (
            ' · <span class="old-order-legend">' +
            integer.format(
              oldOrderTrackings.size
            ) +
            " pedido(s) de fecha anterior resaltado(s)" +
            "</span>"
          )
        : ""
    ) +
    (
      window.__SUPER_DUPAZA_PENDING_DIAGNOSTICS__?.pendingProductLines !==
        undefined
        ? (
            " · D1 líneas: " +
            integer.format(
              Number(
                window.__SUPER_DUPAZA_PENDING_DIAGNOSTICS__
                  .pendingProductLines ||
                0
              )
            )
          )
        : ""
    ) +
    " · UI 5.25.1";


  const columns =
    visiblePendingColumns();


  document
    .getElementById(
      "ordersHead"
    )
    .innerHTML =
    columns
      .map(
        column =>
          "<th>" +
          esc(
            column.label
          ) +
          "</th>"
      )
      .join(
        ""
      ) +
    "<th>Control</th>";


  const orderLevelKeys =
    new Set([
      "customerTrackingCode",
      "taxId",
      "customerName",
      "primaryPhone",
      "secondaryPhone",
      "sellerName",
      "shippingMunicipality",
      "shippingReference",
      "custom_rcf",
      "salesChannel",
      "shippingDepartment",
      "orderDate",
      "comments",
      "trackingCode",
      "warehouse",
      "orderTotal",
      "shippingAvenue",
      "shippingHouseNumber",
      "shippingZone",
      "custom_bodega",
      "custom_canal",
      "custom_payment_method",
      "custom_marca"
    ]);


  document
    .getElementById(
      "ordersBody"
    )
    .innerHTML =
    visibleOrderRows
      .map(
        row => {

          const first =
            Boolean(
              row._firstOrderRow
            );


          const linePosition =
            linePositions.get(
              row
            ) ||
            {
              index:
                1,
              total:
                1
            };


          const oldOrder =
            isPendingOrderFromAnotherDay(
              row,
              todayGt
            );


          const rowClasses =
            [
              first
                ? ""
                : "child-row",

              oldOrder
                ? "pending-old-order"
                : ""
            ]
              .filter(Boolean)
              .join(
                " "
              );


          return (
            '<tr class="' +
            rowClasses +
            '" ' +
            'data-tracking="' +
            esc(
              String(
                row.trackingCode ??
                ""
              )
            ) +
            '" ' +
            'data-line-key="' +
            esc(
              String(
                row._lineKey ??
                ""
              )
            ) +
            '" ' +
            'data-line-index="' +
            esc(
              String(
                linePosition.index
              )
            ) +
            '" ' +
            // No agregamos title al <tr>.
            // La fecha distinta a hoy se comunica únicamente con el
            // color visual pending-old-order, sin tooltip nativo.
            '>' +

            columns
              .map(
                column => {

                  let raw =
                    row[
                      column.key
                    ];


                  if (
                    !first &&
                    orderLevelKeys.has(
                      column.key
                    )
                  ) {

                    raw =
                      "";

                  }


                  let display =
                    raw ??
                    "";


                  if (
                    column.money &&
                    display !==
                      "" &&
                    Number.isFinite(
                      Number(
                        display
                      )
                    )
                  ) {

                    display =
                      money.format(
                        Number(
                          display
                        )
                      );

                  }


                  let copyValue =
                    raw ??
                    "";


                  let message =
                    "Valor copiado";


                  if (
                    column.key ===
                      "sku" &&
                    first
                  ) {

                    copyValue =
                      row._copySkus ||
                      raw ||
                      "";


                    message =
                      "Códigos de la orden copiados";

                  } else if (
                    column.key ===
                    "sku"
                  ) {

                    message =
                      "Código de artículo copiado";

                  }


                  let fullValue =
                    display ===
                      ""
                      ? "(vacío)"
                      : String(
                          display
                        );


                  if (
                    column.key ===
                      "sellerName" &&
                    first &&
                    row._sellerOriginal &&
                    String(
                      row._sellerOriginal
                    )
                      .trim() !==
                      String(
                        row.sellerName ??
                        ""
                      )
                        .trim()
                  ) {

                    fullValue =
                      "Nombre real API: " +
                      String(
                        row._sellerOriginal
                      );

                  }


                  return (
                    '<td ' +
                    'data-copy="' +
                    encodeURIComponent(
                      String(
                        copyValue
                      )
                    ) +
                    '" ' +
                    'data-full="' +
                    encodeURIComponent(
                      fullValue
                    ) +
                    '" ' +
                    'data-copy-message="' +
                    esc(
                      message
                    ) +
                    '">' +
                    esc(
                      display
                    ) +
                    "</td>"
                  );

                }
              )
              .join(
                ""
              ) +

            (
              first
                ? (
                    '<td class="sap-action-cell">' +
                    '<button class="sap-action-btn" ' +
                    'onclick="event.stopPropagation();markOrderInSap(\'' +
                    esc(
                      String(
                        row.trackingCode ??
                        ""
                      )
                    ) +
                    '\')">C en SAP</button>' +
                    "</td>"
                  )
                : (
                    '<td class="sap-action-cell">' +
                    '<span class="line-position">' +
                    'Línea ' +
                    esc(
                      String(
                        linePosition.index
                      )
                    ) +
                    '/' +
                    esc(
                      String(
                        linePosition.total
                      )
                    ) +
                    '</span>' +
                    '</td>'
                  )
            ) +

            "</tr>"
          );

        }
      )
      .join(
        ""
      );


  const renderedDomRows =
    document.querySelectorAll(
      "#ordersBody tr"
    ).length;


  if (
    renderedDomRows !==
    visibleOrderRows.length
  ) {

    console.error(
      "SUPER DUPAZA: diferencia entre filas recibidas y filas DOM",
      {
        received:
          visibleOrderRows.length,
        rendered:
          renderedDomRows
      }
    );


    showNotice(
      "La API devolvió " +
      visibleOrderRows.length +
      " líneas, pero el navegador pintó " +
      renderedDomRows +
      ". Recarga con Ctrl+F5.",
      "bad"
    );

  }


  attachPendingCellInteractions();

  requestAnimationFrame(
    applyPendingCellSize
  );

}


// ============================================================
// RESUMEN
// ============================================================


function localIsoDate(
  date
) {

  return (
    date.getFullYear() +
    "-" +
    String(
      date.getMonth() +
      1
    )
      .padStart(
        2,
        "0"
      ) +
    "-" +
    String(
      date.getDate()
    )
      .padStart(
        2,
        "0"
      )
  );

}



function setQuickRange(
  target,
  mode
) {

  const today =
    new Date();


  let from =
    new Date(
      today
    );


  let to =
    new Date(
      today
    );


  if (
    mode ===
    "7d"
  ) {

    from.setDate(
      from.getDate() -
      6
    );

  } else if (
    mode ===
    "month"
  ) {

    from =
      new Date(
        today.getFullYear(),
        today.getMonth(),
        1
      );

  } else if (
    mode ===
    "prev"
  ) {

    from =
      new Date(
        today.getFullYear(),
        today.getMonth() -
        1,
        1
      );


    to =
      new Date(
        today.getFullYear(),
        today.getMonth(),
        0
      );

  } else if (
    mode ===
    "3m"
  ) {

    from =
      new Date(
        today.getFullYear(),
        today.getMonth() -
        2,
        1
      );

  }


  const prefix =
    target ===
    "adv"
      ? "adv"
      : "sum";


  document
    .getElementById(
      prefix +
      "DateFrom"
    )
    .value =
    localIsoDate(
      from
    );


  document
    .getElementById(
      prefix +
      "DateTo"
    )
    .value =
    localIsoDate(
      to
    );


  if (
    prefix ===
    "adv"
  ) {

    loadAdvisor();

  } else {

    loadSummary();

  }

}



function requireRange(
  fromId,
  toId
) {

  const from =
    document
      .getElementById(
        fromId
      )
      .value;


  const to =
    document
      .getElementById(
        toId
      )
      .value;


  if (
    !from ||
    !to
  ) {

    throw new Error(
      "Selecciona fecha inicial y fecha final y luego pulsa Consultar."
    );

  }

}



async function loadReportOptions() {

  const response =
    await fetch(
      "/api/report-options",
      {
        cache:
          "no-store"
      }
    );


  const data =
    await response.json();


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "Error cargando filtros del reporte."
    );

  }


  reportGroupLabels = {
    ...reportGroupLabels,
    ...(
      data.groupLabels ||
      {}
    )
  };


  updateReportVisualLabels();


  const options =
    data.options ||
    {};


  fillGroupSelect(
    "sumGroup",
    options.groups ||
    [],
    "Todos los grupos"
  );


  fillSelect(
    "sumAdvisor",
    options.advisors ||
    [],
    "Todos los asesores"
  );


  fillSelect(
    "sumType",
    options.types ||
    [],
    "Todos los segmentos"
  );


  fillSelect(
    "sumSource",
    options.sources ||
    [],
    "Todas las fuentes"
  );


  reportOptionsLoaded =
    true;

}



function summaryParams() {

  return {

    date_from:
      document
        .getElementById(
          "sumDateFrom"
        )
        .value,

    date_to:
      document
        .getElementById(
          "sumDateTo"
        )
        .value,

    group:
      document
        .getElementById(
          "sumGroup"
        )
        .value,

    advisor:
      document
        .getElementById(
          "sumAdvisor"
        )
        .value,

    type:
      document
        .getElementById(
          "sumType"
        )
        .value,

    source:
      document
        .getElementById(
          "sumSource"
        )
        .value

  };

}



async function loadSummary(
  loadOptions = false
) {

  requireRange(
    "sumDateFrom",
    "sumDateTo"
  );


  const response =
    await fetch(
      "/api/summary?" +
      queryString(
        summaryParams()
      )
    );


  const data =
    await response.json();


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "Error cargando resumen."
    );

  }


  summaryRows =
    data.rows ||
    [];


  reportGroupLabels = {
    ...reportGroupLabels,
    ...(
      data.groupLabels ||
      {}
    )
  };


  updateReportVisualLabels();


  document
    .getElementById(
      "sumKpiZas"
    )
    .textContent =
    money.format(
      data.kpis?.zasConnect ||
      0
    );


  document
    .getElementById(
      "sumKpiPdv"
    )
    .textContent =
    money.format(
      data.kpis?.pdvAdded ||
      0
    );


  document
    .getElementById(
      "sumKpiTotal"
    )
    .textContent =
    money.format(
      data.kpis?.total ||
      0
    );


  document
    .getElementById(
      "sumKpiOrders"
    )
    .textContent =
    integer.format(
      data.kpis?.orders ||
      0
    );


  document
    .getElementById(
      "sumKpiTarget"
    )
    .textContent =
    money.format(
      Number(
        data.kpis?.target ||
        0
      )
    );


  document
    .getElementById(
      "sumKpiTrend"
    )
    .textContent =
    money.format(
      Number(
        data.kpis?.trend ||
        0
      )
    );


  if (
    data.options &&
    (
      loadOptions ||
      !reportOptionsLoaded
    )
  ) {

    const options =
      data.options ||
      {};


    fillSelect(
      "sumGroup",
      options.groups ||
      [],
      "Todos los grupos"
    );


    fillSelect(
      "sumAdvisor",
      options.advisors ||
      [],
      "Todos los asesores"
    );


    fillSelect(
      "sumType",
      options.types ||
      [],
      "Todos los tipos"
    );


    fillSelect(
      "sumSource",
      options.sources ||
      [],
      "Todas las fuentes"
    );


    reportOptionsLoaded =
      true;

  }


  renderSummaryTable();


  renderChart(
    data.chart ||
    []
  );


  summaryHasExecuted =
    true;

  summaryQueryDirty =
    false;


  document.getElementById(
    "sumRangeStatus"
  ).textContent =
    "Rango consultado: " +
    data.range.from +
    " → " +
    data.range.to;

}



function renderChart(
  rows
) {

  const chart =
    document.getElementById(
      "stackChart"
    );


  const labels =
    document.getElementById(
      "xLabels"
    );


  if (
    !rows.length
  ) {

    chart.innerHTML =
      '<div style="margin:auto;color:#667085">Sin datos para el rango seleccionado.</div>';


    labels.innerHTML =
      "";


    return;

  }


  chart.innerHTML =
    rows
      .map(
        row => {

          const segments =
            [
              [
                "K1",
                row.K1,
                row.amountK1
              ],
              [
                "K2",
                row.K2,
                row.amountK2
              ],
              [
                "K3",
                row.K3,
                row.amountK3
              ]
            ]
              .map(
                (
                  [
                    key,
                    pct,
                    amount
                  ]
                ) => {

                  if (
                    !pct ||
                    pct <=
                    0
                  ) {

                    return "";

                  }


                  const label =
                    pct >=
                    7
                      ? Number(
                          pct
                        )
                          .toFixed(
                            1
                          ) +
                        "%"
                      : "";


                  return (
                    '<div ' +
                    'class="segment ' +
                    key.toLowerCase() +
                    '" ' +
                    'style="height:' +
                    pct +
                    '%" ' +
                    'title="' +
                    (
                      reportGroupLabels[
                        key
                      ] ||
                      key
                    ) +
                    ": " +
                    Number(
                      pct
                    )
                      .toFixed(
                        2
                      ) +
                    "% · " +
                    esc(
                      money.format(
                        amount ||
                        0
                      )
                    ) +
                    '">' +
                    label +
                    "</div>"
                  );

                }
              )
              .join(
                ""
              );


          return (
            '<div class="bar-col" ' +
            'title="' +
            esc(
              row.date
            ) +
            " · Total " +
            esc(
              money.format(
                row.total ||
                0
              )
            ) +
            '">' +

            '<div class="bar-stack">' +
            segments +
            "</div>" +

            "</div>"
          );

        }
      )
      .join(
        ""
      );


  labels.innerHTML =
    rows
      .map(
        row =>
          '<div class="x-label-item" ' +
          'title="' +
          esc(
            money.format(
              row.total ||
              0
            )
          ) +
          '">' +
          esc(
            row.date
          ) +
          "</div>"
      )
      .join(
        ""
      );

}


// ============================================================
// ORDEN MANUAL DE COLUMNAS DE REPORTES
// ============================================================

function reportColumnConfig(
  report
) {

  if (
    report ===
    "summary"
  ) {

    return {
      defs:
        SUMMARY_COLUMN_DEFS,
      order:
        summaryColumnOrder,
      storageKey:
        SUMMARY_COLUMN_STORAGE,
      headerId:
        "summaryHeaderRow"
    };

  }


  return {
    defs:
      ADVISOR_COLUMN_DEFS,
    order:
      advisorColumnOrder,
    storageKey:
      ADVISOR_COLUMN_STORAGE,
    headerId:
      "advisorHeaderRow"
  };

}


function orderedReportColumns(
  report
) {

  const config =
    reportColumnConfig(
      report
    );

  const byKey =
    new Map(
      config.defs.map(
        column => [
          column.key,
          column
        ]
      )
    );

  return config.order
    .map(
      key =>
        byKey.get(key)
    )
    .filter(Boolean);

}


function persistReportColumnOrder(
  report
) {

  const config =
    reportColumnConfig(
      report
    );

  try {

    localStorage.setItem(
      config.storageKey,
      JSON.stringify(
        config.order
      )
    );

  } catch (error) {

    console.warn(
      "No se pudo guardar el orden de columnas:",
      error
    );

  }

}


function renderReportHeader(
  report
) {

  const config =
    reportColumnConfig(
      report
    );

  const row =
    document.getElementById(
      config.headerId
    );

  if (!row) {
    return;
  }


  row.innerHTML =
    orderedReportColumns(report)
      .map(
        column =>
          '<th class="report-draggable-th" ' +
          'draggable="true" ' +
          'data-report="' +
          esc(report) +
          '" ' +
          'data-column-key="' +
          esc(column.key) +
          '" ' +
          (
            column.title
              ? 'title="' +
                esc(
                  column.title +
                  " · Arrastra para cambiar de posición"
                ) +
                '" '
              : 'title="Arrastra para cambiar de posición" '
          ) +
          '>' +
          esc(column.label) +
          '</th>'
      )
      .join("");


  row
    .querySelectorAll(
      ".report-draggable-th"
    )
    .forEach(
      th => {

        th.addEventListener(
          "dragstart",
          event => {

            reportColumnDrag = {
              report:
                th.dataset.report,
              key:
                th.dataset.columnKey
            };

            th.classList.add(
              "dragging"
            );

            if (
              event.dataTransfer
            ) {

              event.dataTransfer.effectAllowed =
                "move";

              event.dataTransfer.setData(
                "text/plain",
                th.dataset.columnKey ||
                ""
              );

            }

          }
        );


        th.addEventListener(
          "dragover",
          event => {

            if (
              !reportColumnDrag ||
              reportColumnDrag.report !==
                th.dataset.report
            ) {
              return;
            }

            event.preventDefault();

            th.classList.add(
              "drag-over"
            );

            if (
              event.dataTransfer
            ) {
              event.dataTransfer.dropEffect =
                "move";
            }

          }
        );


        th.addEventListener(
          "dragleave",
          () => {
            th.classList.remove(
              "drag-over"
            );
          }
        );


        th.addEventListener(
          "drop",
          event => {

            event.preventDefault();

            th.classList.remove(
              "drag-over"
            );

            if (
              !reportColumnDrag ||
              reportColumnDrag.report !==
                th.dataset.report
            ) {
              return;
            }


            moveReportColumn(
              reportColumnDrag.report,
              reportColumnDrag.key,
              th.dataset.columnKey
            );

          }
        );


        th.addEventListener(
          "dragend",
          () => {

            row
              .querySelectorAll(
                ".report-draggable-th"
              )
              .forEach(
                cell => {
                  cell.classList.remove(
                    "dragging",
                    "drag-over"
                  );
                }
              );

            reportColumnDrag =
              null;

          }
        );

      }
    );

}


function moveReportColumn(
  report,
  sourceKey,
  targetKey
) {

  if (
    !sourceKey ||
    !targetKey ||
    sourceKey === targetKey
  ) {
    return;
  }


  const config =
    reportColumnConfig(
      report
    );

  const fromIndex =
    config.order.indexOf(
      sourceKey
    );

  const toIndex =
    config.order.indexOf(
      targetKey
    );

  if (
    fromIndex < 0 ||
    toIndex < 0
  ) {
    return;
  }


  config.order.splice(
    fromIndex,
    1
  );

  config.order.splice(
    toIndex,
    0,
    sourceKey
  );


  persistReportColumnOrder(
    report
  );

  renderReportHeader(
    report
  );


  if (
    report ===
    "summary"
  ) {
    renderSummaryTable();
  } else {
    renderAdvisor();
  }

}


function resetReportColumnOrder(
  report
) {

  if (
    report ===
    "summary"
  ) {

    summaryColumnOrder =
      SUMMARY_COLUMN_DEFS.map(
        column =>
          column.key
      );

  } else {

    advisorColumnOrder =
      ADVISOR_COLUMN_DEFS.map(
        column =>
          column.key
      );

  }


  persistReportColumnOrder(
    report
  );

  renderReportHeader(
    report
  );


  if (
    report ===
    "summary"
  ) {
    renderSummaryTable();
  } else {
    renderAdvisor();
  }


  showNotice(
    "Orden de columnas restablecido.",
    "ok"
  );

}


function reportCellHtml(
  report,
  row,
  column
) {

  const raw =
    row[
      column.key
    ];


  if (
    !column.money
  ) {

    return (
      '<td class="report-text-cell">' +
      esc(
        raw ??
        ""
      ) +
      '</td>'
    );

  }


  const strong =
    (
      report ===
        "advisor" &&
      (
        column.key ===
          "MONTO" ||
        column.key ===
          "META"
      )
    )
      ? " strong"
      : "";


  return (
    '<td class="report-money-cell money' +
    strong +
    '">' +
    money.format(
      Number(
        raw ||
        0
      )
    ) +
    '</td>'
  );

}


function renderSummaryTable() {

  const body =
    document.getElementById(
      "summaryBody"
    );

  if (!body) {
    return;
  }


  const columns =
    orderedReportColumns(
      "summary"
    );


  body.innerHTML =
    summaryRows
      .map(
        row => {

          const total =
            row.grupo ===
            "RESUMEN";


          return (
            '<tr class="' +
            (
              total
                ? "total"
                : ""
            ) +
            '">' +
            columns
              .map(
                column =>
                  reportCellHtml(
                    "summary",
                    row,
                    column
                  )
              )
              .join("") +
            '</tr>'
          );

        }
      )
      .join("");

}


// ============================================================
// ASESOR
// ============================================================

function advisorParams() {

  return {

    date_from:
      document
        .getElementById(
          "advDateFrom"
        )
        .value,

    date_to:
      document
        .getElementById(
          "advDateTo"
        )
        .value

  };

}



async function loadAdvisor() {

  requireRange(
    "advDateFrom",
    "advDateTo"
  );


  const response =
    await fetch(
      "/api/advisor-summary?" +
      queryString(
        advisorParams()
      )
    );


  const data =
    await response.json();


  if (
    !response.ok ||
    !data.ok
  ) {

    throw new Error(
      data.error ||
      "Error cargando resumen por asesor."
    );

  }


  advisorRows =
    data.rows ||
    [];


  document
    .getElementById(
      "advKpiAdvisors"
    )
    .textContent =
    integer.format(
      data.kpis?.advisors ||
      0
    );


  document
    .getElementById(
      "advKpiTotal"
    )
    .textContent =
    money.format(
      data.kpis?.total ||
      0
    );


  document
    .getElementById(
      "advKpiDigital"
    )
    .textContent =
    money.format(
      data.kpis?.digital ||
      0
    );


  document
    .getElementById(
      "advKpiPiso"
    )
    .textContent =
    money.format(
      Number(
        data.kpis?.petapa ||
        0
      ) +
      Number(
        data.kpis?.xenacoj ||
        0
      )
    );


  document
    .getElementById(
      "advKpiTarget"
    )
    .textContent =
    money.format(
      Number(
        data.kpis?.target ||
        0
      )
    );


  document
    .getElementById(
      "advKpiTrend"
    )
    .textContent =
    money.format(
      Number(
        data.kpis?.trend ||
        0
      )
    );


  advisorHasExecuted =
    true;

  advisorQueryDirty =
    false;


  document.getElementById(
    "advRangeStatus"
  ).textContent =
    "Rango consultado: " +
    data.range.from +
    " → " +
    data.range.to;


  renderAdvisor();

}



function renderAdvisor() {

  const search =
    (
      document
        .getElementById(
          "advSearch"
        )
        .value ||
      ""
    )
      .trim()
      .toLowerCase();


  const rows =
    advisorRows.filter(
      row =>
        row.ASESOR ===
          "TOTAL" ||
        !search ||
        String(
          row.ASESOR ??
          ""
        )
          .toLowerCase()
          .includes(
            search
          )
    );


  const columns =
    orderedReportColumns(
      "advisor"
    );


  document
    .getElementById(
      "advisorBody"
    )
    .innerHTML =
    rows
      .map(
        row => {

          const total =
            row.ASESOR ===
            "TOTAL";


          return (
            '<tr class="' +
            (
              total
                ? "total"
                : ""
            ) +
            '">' +
            columns
              .map(
                column =>
                  reportCellHtml(
                    "advisor",
                    row,
                    column
                  )
              )
              .join("") +
            '</tr>'
          );

        }
      )
      .join(
        ""
      );

}


// ============================================================
// CSV
// ============================================================

function csvCell(
  value
) {

  const text =
    String(
      value ??
      ""
    );


  return (
    '"' +
    text.replace(
      /"/g,
      '""'
    ) +
    '"'
  );

}



function downloadCsv(
  filename,
  headers,
  rows
) {

  const csv =
    "\uFEFF" +
    [
      headers
        .map(
          csvCell
        )
        .join(
          ","
        ),

      ...rows.map(
        row =>
          row
            .map(
              csvCell
            )
            .join(
              ","
            )
      )
    ]
      .join(
        "\r\n"
      );


  const blob =
    new Blob(
      [
        csv
      ],
      {
        type:
          "text/csv;charset=utf-8"
      }
    );


  const url =
    URL.createObjectURL(
      blob
    );


  const a =
    document.createElement(
      "a"
    );


  a.href =
    url;


  a.download =
    filename;


  document.body.appendChild(
    a
  );


  a.click();


  a.remove();


  URL.revokeObjectURL(
    url
  );

}



async function exportNotLoadedExcel() {

  try {

    if (
      typeof ExcelJS ===
      "undefined"
    ) {

      throw new Error(
        "No se pudo cargar el generador de Excel. Recarga la página e inténtalo nuevamente."
      );

    }


    if (
      !visibleOrderRows.length
    ) {

      showNotice(
        "No hay órdenes pendientes visibles para exportar.",
        "warn"
      );

      return;

    }


    // --------------------------------------------------------
    // COLUMNAS DEL EXCEL
    // Datos generales: una sola vez por pedido.
    // Productos: Código artículo / Descripción / Cantidad.
    // --------------------------------------------------------

    const configuredColumns =
      visiblePendingColumns();


    // Esta lista debe existir dentro del JavaScript del navegador.
    // ORDER_LEVEL_KEYS existe en el Worker, pero no dentro del HTML
    // generado; por eso antes aparecía "ORDER_LEVEL_KEYS is not defined".
    const excelOrderLevelKeys =
      new Set([
        "customerTrackingCode",
        "taxId",
        "customerName",
        "primaryPhone",
        "secondaryPhone",
        "sellerName",
        "shippingMunicipality",
        "shippingReference",
        "custom_rcf",
        "salesChannel",
        "shippingDepartment",
        "orderDate",
        "comments",
        "trackingCode",
        "warehouse",
        "orderTotal",
        "shippingAvenue",
        "shippingHouseNumber",
        "shippingZone",
        "custom_bodega",
        "custom_canal",
        "custom_payment_method",
        "custom_marca"
      ]);


    const generalColumns =
      configuredColumns.filter(
        column =>
          excelOrderLevelKeys.has(
            column.key
          )
      );


    const requiredProductKeys = [
      "sku",
      "description",
      "quantity"
    ];


    const productColumns =
      requiredProductKeys
        .map(
          key =>
            orderColumns.find(
              column =>
                column.key ===
                key
            )
        )
        .filter(Boolean);


    const exportColumns = [
      ...generalColumns,
      ...productColumns
    ];


    if (
      !exportColumns.length
    ) {

      throw new Error(
        "No hay columnas disponibles para generar el Excel."
      );

    }


    // --------------------------------------------------------
    // AGRUPAR POR PEDIDO
    // --------------------------------------------------------

    const orderGroups =
      new Map();


    for (
      const row
      of visibleOrderRows
    ) {

      const tracking =
        String(
          row.trackingCode ??
          ""
        );


      if (
        !orderGroups.has(
          tracking
        )
      ) {

        orderGroups.set(
          tracking,
          []
        );

      }


      orderGroups
        .get(
          tracking
        )
        .push(
          row
        );

    }


    const exportRows = [];


    for (
      const [
        tracking,
        groupRows
      ]
      of orderGroups.entries()
    ) {

      if (
        !groupRows.length
      ) {
        continue;
      }


      groupRows.forEach(
        (
          row,
          index
        ) => {

          const isFirst =
            index ===
            0;


          const values =
            exportColumns.map(
              column => {

                if (
                  requiredProductKeys.includes(
                    column.key
                  )
                ) {

                  return (
                    row[
                      column.key
                    ] ??
                    ""
                  );

                }


                if (
                  isFirst
                ) {

                  return (
                    row[
                      column.key
                    ] ??
                    ""
                  );

                }


                return "";

              }
            );


          exportRows.push({
            tracking,
            isFirst,
            hasChildren:
              groupRows.length >
              1,
            values
          });

        }
      );

    }


    const workbook =
      new ExcelJS.Workbook();


    workbook.creator =
      "SUPER DUPAZA ONLINE";


    workbook.created =
      new Date();


    const worksheet =
      workbook.addWorksheet(
        "Órdenes no cargadas",
        {
          views: [
            {
              state:
                "frozen",
              ySplit:
                1
            }
          ]
        }
      );


    // Escribimos celdas normales + AutoFilter.
    // No usamos una Tabla estructurada de Excel.
    worksheet.addRow(
      exportColumns.map(
        column =>
          column.label
      )
    );


    for (
      const item
      of exportRows
    ) {

      worksheet.addRow(
        item.values
      );

    }


    // --------------------------------------------------------
    // ENCABEZADO
    // --------------------------------------------------------

    const headerRow =
      worksheet.getRow(
        1
      );


    headerRow.height =
      27;


    headerRow.eachCell(
      {
        includeEmpty:
          true
      },
      cell => {

        cell.fill = {
          type:
            "pattern",
          pattern:
            "solid",
          fgColor: {
            argb:
              "FF202020"
          }
        };


        cell.font = {
          bold:
            true,
          color: {
            argb:
              "FFFFFFFF"
          }
        };


        cell.alignment = {
          vertical:
            "middle",
          horizontal:
            "center",
          wrapText:
            true
        };


        cell.border = {
          bottom: {
            style:
              "thin",
            color: {
              argb:
                "FF808080"
            }
          }
        };

      }
    );


    const groupFills = [
      "FFFFFBEA",
      "FFF3F6F9"
    ];


    const skuColumnIndex =
      exportColumns.findIndex(
        column =>
          column.key ===
          "sku"
      ) +
      1;


    const descriptionColumnIndex =
      exportColumns.findIndex(
        column =>
          column.key ===
          "description"
      ) +
      1;


    const quantityColumnIndex =
      exportColumns.findIndex(
        column =>
          column.key ===
          "quantity"
      ) +
      1;


    let groupIndex =
      -1;

    let previousTracking =
      null;


    for (
      let i = 0;
      i <
      exportRows.length;
      i++
    ) {

      const item =
        exportRows[i];


      if (
        item.tracking !==
        previousTracking
      ) {

        groupIndex++;

        previousTracking =
          item.tracking;

      }


      const excelRow =
        worksheet.getRow(
          i +
          2
        );


      excelRow.height =
        21;


      excelRow.eachCell(
        {
          includeEmpty:
            true
        },
        cell => {

          cell.fill = {
            type:
              "pattern",
            pattern:
              "solid",
            fgColor: {
              argb:
                groupFills[
                  groupIndex %
                  groupFills.length
                ]
            }
          };


          cell.alignment = {
            vertical:
              "top",
            wrapText:
              true
          };


          cell.border = {
            bottom: {
              style:
                "hair",
              color: {
                argb:
                  "FFD9DDE3"
              }
            }
          };

        }
      );


      if (
        item.isFirst
      ) {

        excelRow.eachCell(
          {
            includeEmpty:
              true
          },
          cell => {

            cell.border = {
              ...cell.border,

              top: {
                style:
                  "thin",
                color: {
                  argb:
                    "FF9CA3AF"
                }
              }
            };

          }
        );

      }


      // Si hay combo/múltiples líneas, la primera queda como
      // producto principal y las demás abajo.
      if (
        item.isFirst &&
        item.hasChildren
      ) {

        for (
          const columnIndex
          of [
            skuColumnIndex,
            descriptionColumnIndex,
            quantityColumnIndex
          ]
        ) {

          if (
            columnIndex >
            0
          ) {

            const cell =
              excelRow.getCell(
                columnIndex
              );


            cell.font = {
              ...cell.font,
              bold:
                true
            };

          }

        }

      }


      // Líneas hijas: solo Código / Descripción / Cantidad.
      // Se indenta visualmente sin modificar el valor del SKU.
      if (
        !item.isFirst
      ) {

        for (
          const columnIndex
          of [
            skuColumnIndex,
            descriptionColumnIndex
          ]
        ) {

          if (
            columnIndex >
            0
          ) {

            const cell =
              excelRow.getCell(
                columnIndex
              );


            cell.alignment = {
              ...cell.alignment,
              indent:
                1
            };

          }

        }

      }

    }


    // --------------------------------------------------------
    // FORMATOS Y ANCHOS
    // --------------------------------------------------------

    const moneyKeys =
      new Set(
        exportColumns
          .filter(
            column =>
              column.money
          )
          .map(
            column =>
              column.key
          )
      );


    exportColumns.forEach(
      (
        column,
        index
      ) => {

        const excelColumn =
          worksheet.getColumn(
            index +
            1
          );


        let maxLength =
          Math.max(
            10,
            String(
              column.label
            ).length
          );


        for (
          const item
          of exportRows
        ) {

          const value =
            item.values[
              index
            ];


          const length =
            String(
              value ??
              ""
            )
              .length;


          if (
            length >
            maxLength
          ) {

            maxLength =
              length;

          }

        }


        if (
          column.key ===
          "sku"
        ) {

          excelColumn.width =
            20;

        } else if (
          column.key ===
          "description"
        ) {

          excelColumn.width =
            38;

        } else if (
          column.key ===
          "quantity"
        ) {

          excelColumn.width =
            12;

        } else {

          excelColumn.width =
            Math.min(
              Math.max(
                maxLength +
                2,
                12
              ),
              32
            );

        }


        if (
          moneyKeys.has(
            column.key
          )
        ) {

          excelColumn.numFmt =
            '"Q" #,##0.00';

        }

      }
    );


    // AutoFilter normal de hoja, sin XML de tabla estructurada.
    worksheet.autoFilter = {
      from: {
        row:
          1,
        column:
          1
      },

      to: {
        row:
          Math.max(
            1,
            exportRows.length +
            1
          ),
        column:
          exportColumns.length
      }
    };


    const buffer =
      await workbook.xlsx
        .writeBuffer();


    const blob =
      new Blob(
        [
          buffer
        ],
        {
          type:
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
      );


    const now =
      new Date();


    const parts =
      new Intl.DateTimeFormat(
        "en-US",
        {
          timeZone:
            "America/Guatemala",
          day:
            "2-digit",
          month:
            "2-digit",
          year:
            "numeric",
          hour:
            "2-digit",
          hour12:
            true
        }
      )
        .formatToParts(
          now
        );


    const getPart =
      type =>
        parts.find(
          part =>
            part.type ===
            type
        )?.value ||
        "";


    const filename =
      "ordenes no cargadas " +
      getPart(
        "day"
      ) +
      "-" +
      getPart(
        "month"
      ) +
      "-" +
      getPart(
        "year"
      ) +
      " " +
      getPart(
        "hour"
      ) +
      " " +
      getPart(
        "dayPeriod"
      )
        .toUpperCase() +
      ".xlsx";


    const url =
      URL.createObjectURL(
        blob
      );


    const a =
      document.createElement(
        "a"
      );


    a.href =
      url;


    a.download =
      filename;


    document.body.appendChild(
      a
    );


    a.click();


    a.remove();


    URL.revokeObjectURL(
      url
    );


    showNotice(
      "Excel generado: " +
      filename,
      "ok"
    );


  } catch (error) {

    showNotice(
      error.message,
      "bad"
    );

  }

}



async function downloadReportExcel(
  filename,
  title,
  rangeText,
  columns,
  rows
) {

  if (
    typeof ExcelJS ===
    "undefined"
  ) {

    throw new Error(
      "No se pudo cargar el generador de Excel."
    );

  }


  const workbook =
    new ExcelJS.Workbook();


  workbook.creator =
    "SUPER DUPAZA";


  const worksheet =
    workbook.addWorksheet(
      "Reporte"
    );


  worksheet.mergeCells(
    1,
    1,
    1,
    columns.length
  );


  const titleCell =
    worksheet.getCell(
      1,
      1
    );


  titleCell.value =
    title;

  titleCell.font = {
    bold:true,
    size:16
  };


  worksheet.mergeCells(
    2,
    1,
    2,
    columns.length
  );


  worksheet.getCell(
    2,
    1
  ).value =
    rangeText;


  const headerRow =
    worksheet.addRow([]);

  headerRow.values =
    [
      null,
      ...columns.map(
        column =>
          column.label
      )
    ];


  headerRow.font = {
    bold:true,
    color:{
      argb:
        "FFFFFFFF"
    }
  };


  headerRow.eachCell(
    {
      includeEmpty:true
    },
    cell => {

      cell.fill = {
        type:
          "pattern",
        pattern:
          "solid",
        fgColor:{
          argb:
            "FF202020"
        }
      };

    }
  );


  for (
    const row
    of rows
  ) {

    const excelRow =
      worksheet.addRow(
        columns.map(
          column =>
            row[
              column.key
            ] ??
            ""
        )
      );


    excelRow.eachCell(
      cell => {
        cell.border = {
          bottom:{
            style:
              "hair",
            color:{
              argb:
                "FFD9DDE3"
            }
          }
        };
      }
    );

  }


  columns.forEach(
    (column, index) => {

      const excelColumn =
        worksheet.getColumn(
          index + 1
        );


      excelColumn.width =
        column.width ||
        18;


      if (
        column.money
      ) {

        excelColumn.numFmt =
          '"Q" #,##0.00';

      }

    }
  );


  worksheet.views = [
    {
      state:
        "frozen",
      ySplit:
        3
    }
  ];


  worksheet.autoFilter = {
    from:{
      row:3,
      column:1
    },
    to:{
      row:
        Math.max(
          3,
          rows.length +
          3
        ),
      column:
        columns.length
    }
  };


  const buffer =
    await workbook.xlsx
      .writeBuffer();


  const blob =
    new Blob(
      [buffer],
      {
        type:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      }
    );


  const url =
    URL.createObjectURL(
      blob
    );


  const a =
    document.createElement(
      "a"
    );


  a.href =
    url;

  a.download =
    filename;


  document.body.appendChild(
    a
  );

  a.click();
  a.remove();


  URL.revokeObjectURL(
    url
  );

}



async function exportSummaryExcel() {

  try {

    if (
      !summaryRows.length
    ) {
      throw new Error(
        "Primero consulta un rango para exportarlo."
      );
    }


    const params =
      summaryParams();


    await downloadReportExcel(
      "resumen_bo_vt_" +
      params.date_from +
      "_" +
      params.date_to +
      ".xlsx",
      "SUPER DUPAZA · Resumen BO / VT",
      "Rango: " +
      params.date_from +
      " → " +
      params.date_to,
      orderedReportColumns(
        "summary"
      ),
      summaryRows
    );


    showNotice(
      "Excel de Resumen BO / VT generado.",
      "ok"
    );

  } catch (error) {

    showNotice(
      error.message,
      "bad"
    );

  }

}



async function exportAdvisorExcel() {

  try {

    if (
      !advisorRows.length
    ) {
      throw new Error(
        "Primero consulta un rango para exportarlo."
      );
    }


    const params =
      advisorParams();


    await downloadReportExcel(
      "resumen_por_asesor_" +
      params.date_from +
      "_" +
      params.date_to +
      ".xlsx",
      "SUPER DUPAZA · Resumen por asesor",
      "Rango: " +
      params.date_from +
      " → " +
      params.date_to,
      orderedReportColumns(
        "advisor"
      ),
      advisorRows
    );


    showNotice(
      "Excel de Resumen por asesor generado.",
      "ok"
    );

  } catch (error) {

    showNotice(
      error.message,
      "bad"
    );

  }

}



function exportSummaryCsv() {

  downloadCsv(

    "resumen_bo_vt.csv",

    [
      "GRUPO",
      "DIGITAL",
      "PETAPA",
      "XENACOJ",
      "TOTAL"
    ],

    summaryRows.map(
      row => [
        row.grupo,
        row.DIGITAL,
        row.PETAPA,
        row.XENACOJ,
        row.TOTAL
      ]
    )

  );

}



function exportAdvisorCsv() {

  downloadCsv(

    "resumen_por_asesor.csv",

    [
      "ASESOR",
      "MONTO",
      "DIGITAL",
      "PETAPA",
      "XENACOJ",
      "META",
      "TENDENCIA"
    ],

    advisorRows.map(
      row => [
        row.ASESOR,
        row.MONTO,
        row.DIGITAL,
        row.PETAPA,
        row.XENACOJ,
        row.META,
        row.TENDENCIA
      ]
    )

  );

}


// ============================================================
// ACTUALIZAR VISTA
// ============================================================

async function refreshView() {

  setBusy(
    true
  );


  showNotice(
    ""
  );


  try {

    await loadStatus();

    const jobs = [
      loadNotLoaded(),
      loadReportOptions()
    ];


    if (
      document
        .getElementById(
          "sumDateFrom"
        )
        .value &&
      document
        .getElementById(
          "sumDateTo"
        )
        .value &&
      !summaryQueryDirty
    ) {

      jobs.push(
        loadSummary()
      );

    }


    if (
      document
        .getElementById(
          "advDateFrom"
        )
        .value &&
      document
        .getElementById(
          "advDateTo"
        )
        .value &&
      !advisorQueryDirty
    ) {

      jobs.push(
        loadAdvisor()
      );

    }


    await Promise.all(
      jobs
    );


    showNotice(
      "Vista actualizada desde D1.",
      "ok"
    );


  } catch (error) {

    showNotice(
      error.message,
      "bad"
    );


  } finally {

    setBusy(
      false
    );

  }

}


// ============================================================
// INICIO
// ============================================================

(async function init() {

  renderReportHeader(
    "summary"
  );

  renderReportHeader(
    "advisor"
  );

  setupReportDateUx();


  setBusy(
    true
  );


  try {

    await loadStatus();


    await Promise.all([
      loadNotLoaded(),
      loadReportOptions()
    ]);


    showNotice(
      "D1 listo. Al completar la fecha final se ejecutará la primera consulta automáticamente.",
      "ok"
    );


  } catch (error) {

    showNotice(
      error.message,
      "bad"
    );


  } finally {

    setBusy(
      false
    );


    if (
      !autoRefreshTimer
    ) {

      autoRefreshTimer =
        setInterval(
          async () => {

            try {

              await loadStatus();

              await loadNotLoaded();


              if (
                document
                  .getElementById(
                    "sumDateFrom"
                  )
                  .value &&
                document
                  .getElementById(
                    "sumDateTo"
                  )
                  .value &&
                !summaryQueryDirty
              ) {

                await loadSummary();

              }


              if (
                document
                  .getElementById(
                    "advDateFrom"
                  )
                  .value &&
                document
                  .getElementById(
                    "advDateTo"
                  )
                  .value &&
                !advisorQueryDirty
              ) {

                await loadAdvisor();

              }

            } catch (
              error
            ) {

              console.error(
                "Auto refresh:",
                error
              );

            }

          },
          300000
        );

    }


    window.addEventListener(
      "resize",
      applyPendingCellSize
    );

  }

})();

</script>

</body>

</html>`,
    {
      headers: {
        "content-type":
          "text/html; charset=UTF-8",

        "Cache-Control":
          "no-store"
      }
    }
  );

}