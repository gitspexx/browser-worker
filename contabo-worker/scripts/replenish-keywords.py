#!/usr/bin/env python3
"""
Replenish botsol keyword fuel WITHOUT the duplicate trap.
Round-1 already scraped the high-yield terms across each country's original ~10 cities,
so re-running term x same-city = pure dup. Fresh, non-dup fuel = high-yield terms x NEW
secondary cities (not yet scraped). Writes chunked ~24-keyword files into
keywords_v2/<country>/<cat>_<round>_<n>.txt so the agent picks them up as fresh active work.

Env knobs:
  BOTSOL_ROUND     round tag for filenames + city-list selection (default "r2")
  ONLY_COUNTRIES   comma-separated country dir names; when set, skip everything else
  ONLY_CATS        comma-separated category names; when set, skip everything else
Never overwrites: a file already present in the country dir, its done/ or quarantine/
subdir is skipped with a warning.
"""
import os, random

ROUND = os.environ.get("BOTSOL_ROUND", "r2")
# seed varies by round so r3 shuffles differ from r2 (random.seed(str) is deterministic)
random.seed("42-" + ROUND)
BASE = r"C:\Botsol\pipeline\keywords_v2"

ONLY_COUNTRIES = set(x.strip() for x in os.environ.get("ONLY_COUNTRIES", "").split(",") if x.strip())
ONLY_CATS = set(x.strip() for x in os.environ.get("ONLY_CATS", "").split(",") if x.strip())

# Proven high-yield terms (from the pruned round-1 analysis) — the keepers only.
HIGH_YIELD = {
    "eat":      ["restaurant", "seafood restaurant", "steakhouse", "local food", "traditional restaurant", "fine dining", "street food"],
    "cafe":     ["coffee shop", "cafe", "specialty coffee", "bakery cafe", "brunch cafe", "dessert cafe"],
    "drink":    ["bar", "cocktail bar", "rooftop bar", "wine bar", "brewery", "pub", "nightclub"],
    "stay":     ["hotel", "boutique hotel", "hostel", "resort", "guest house", "bed and breakfast"],
    "explore":  ["tour", "city tour", "walking tour", "food tour", "boat tour", "day trip", "sightseeing tour"],
    "do":       ["adventure tour", "cooking class", "museum", "cultural tour", "excursion", "art gallery"],
    "wellness": ["spa", "yoga studio", "massage", "wellness center", "thermal bath"],
    "essentials": ["pharmacy", "supermarket", "gym", "coworking space", "laundry service", "car rental"],
}

# NEW cities per round (beyond the ~10 already scraped in round-1). Country dir name -> cities.
NEW_CITIES_BY_ROUND = {
    "r2": {
    "albania":     ["Fier", "Elbasan", "Lushnje", "Kavaje", "Lezhe", "Kukes", "Patos", "Peshkopi", "Kruje", "Burrel", "Librazhd", "Gramsh", "Ballsh", "Corovode", "Permet", "Delvine", "Bajram Curri", "Rreshen"],
    "italy":       ["Bologna", "Verona", "Genoa", "Bari", "Palermo", "Catania", "Turin", "Padua", "Perugia", "Lecce", "Siena", "Pisa", "Bergamo", "Modena", "Parma", "Trieste", "Rimini", "Sorrento"],
    "spain":       ["Zaragoza", "Malaga", "Murcia", "Bilbao", "Alicante", "Cordoba", "Valladolid", "Vigo", "Gijon", "Santander", "Pamplona", "Salamanca", "San Sebastian", "Cadiz", "Tarragona", "Logrono", "Toledo", "Girona"],
    "germany":     ["Cologne", "Frankfurt", "Stuttgart", "Dusseldorf", "Leipzig", "Dresden", "Hannover", "Nuremberg", "Bremen", "Bonn", "Heidelberg", "Freiburg", "Mainz", "Augsburg", "Munster", "Karlsruhe", "Wiesbaden", "Regensburg"],
    "portugal":    ["Braga", "Coimbra", "Funchal", "Aveiro", "Evora", "Faro", "Guimaraes", "Cascais", "Sintra", "Lagos", "Setubal", "Viseu", "Ponta Delgada", "Tomar", "Tavira", "Nazare", "Obidos", "Viana do Castelo"],
    "japan":       ["Nagoya", "Kobe", "Yokohama", "Kanazawa", "Takayama", "Matsumoto", "Sendai", "Okinawa", "Nagasaki", "Kumamoto", "Beppu", "Hakodate", "Otaru", "Nikko", "Kawagoe", "Uji", "Himeji", "Matsuyama"],
    "thailand":    ["Hua Hin", "Kanchanaburi", "Sukhothai", "Koh Tao", "Koh Chang", "Phi Phi", "Khao Lak", "Nan", "Udon Thani", "Trang", "Lampang", "Hat Yai", "Chumphon", "Koh Kood", "Khao Sok", "Mae Hong Son", "Lopburi", "Surat Thani"],
    "vietnam":     ["Hanoi", "Da Nang", "Hoi An", "Nha Trang", "Hue", "Da Lat", "Sapa", "Ha Long", "Can Tho", "Vung Tau", "Phu Quoc", "Ninh Binh", "Quy Nhon", "Mui Ne", "Hai Phong", "Buon Ma Thuot", "Pleiku", "Ha Giang"],
    "indonesia":   ["Yogyakarta", "Surabaya", "Bandung", "Ubud", "Canggu", "Lombok", "Medan", "Makassar", "Malang", "Semarang", "Labuan Bajo", "Gili Trawangan", "Bogor", "Solo", "Manado", "Padang", "Balikpapan", "Banyuwangi"],
    "india":       ["Jaipur", "Udaipur", "Agra", "Varanasi", "Kochi", "Goa", "Rishikesh", "Amritsar", "Pune", "Ahmedabad", "Jodhpur", "Mysore", "Pondicherry", "Hyderabad", "Kolkata", "Darjeeling", "Shimla", "Munnar"],
    "turkey":      ["Antalya", "Izmir", "Bodrum", "Cappadocia", "Bursa", "Konya", "Gaziantep", "Fethiye", "Trabzon", "Eskisehir", "Kas", "Marmaris", "Pamukkale", "Sanliurfa", "Mardin", "Alanya", "Canakkale", "Cesme"],
    "morocco":     ["Marrakech", "Fes", "Chefchaouen", "Essaouira", "Tangier", "Rabat", "Agadir", "Meknes", "Ouarzazate", "Merzouga", "Asilah", "Tetouan", "El Jadida", "Ifrane", "Taghazout", "Dakhla", "Azrou", "Setti Fatma"],
    "egypt":       ["Alexandria", "Luxor", "Aswan", "Hurghada", "Sharm El Sheikh", "Dahab", "Giza", "Marsa Alam", "Siwa", "Port Said", "Suez", "Tanta", "Ismailia", "El Gouna", "Nuweiba", "Fayoum", "Mansoura", "Sohag"],
    "mexico":      ["Merida", "Queretaro", "Morelia", "Tulum", "San Cristobal", "Guanajuato", "Zacatecas", "Campeche", "Veracruz", "Cuernavaca", "Toluca", "Aguascalientes", "Pachuca", "Xalapa", "Mazatlan", "La Paz", "Bacalar", "Sayulita"],
    "philippines": ["Cebu", "Davao", "Bohol", "Palawan", "Boracay", "Baguio", "Iloilo", "Vigan", "Siargao", "Dumaguete", "Tagaytay", "Bacolod", "Cagayan de Oro", "Zamboanga", "Legazpi", "Coron", "El Nido", "Sagada"],
    # --- r2 wave 2: countries whose round-1 used the generator's cities[0:10] ---
    "ecuador":            ["Loja", "Ambato", "Riobamba", "Ibarra", "Tena", "Puyo", "Machala", "Esmeraldas", "Atacames", "Vilcabamba", "Latacunga", "Puerto Lopez"],
    "south_korea":        ["Daejeon", "Gwangju", "Ulsan", "Changwon", "Cheongju", "Chuncheon", "Andong", "Pohang", "Yeosu", "Tongyeong", "Mokpo", "Suncheon", "Jinju", "Cheonan"],
    "china":              ["Guangzhou", "Shenzhen", "Chongqing", "Nanjing", "Wuhan", "Qingdao", "Xiamen", "Dali", "Harbin", "Tianjin", "Zhangjiajie", "Yangshuo", "Shenyang", "Changsha", "Sanya", "Ningbo"],
    "taiwan":             ["Keelung", "Taitung", "Yilan", "Chiayi", "Hsinchu", "Taoyuan", "Pingtung", "Changhua", "Lukang", "Tamsui", "Beitou", "Green Island"],
    "dominican_republic": ["Bavaro", "Boca Chica", "Juan Dolio", "Bayahibe", "Constanza", "Barahona", "Las Galeras", "Rio San Juan", "Higuey", "San Pedro de Macoris", "Moca", "Pedernales"],
    "romania":            ["Oradea", "Craiova", "Galati", "Ploiesti", "Targu Mures", "Suceava", "Baia Mare", "Alba Iulia", "Pitesti", "Arad", "Predeal", "Busteni", "Mamaia", "Vama Veche"],
    "serbia":             ["Pancevo", "Cacak", "Kraljevo", "Leskovac", "Valjevo", "Uzice", "Sombor", "Zrenjanin", "Sremski Karlovci", "Vranje", "Sabac", "Novi Pazar"],
    "russia":             ["Samara", "Rostov-on-Don", "Krasnodar", "Ufa", "Perm", "Volgograd", "Voronezh", "Krasnoyarsk", "Tyumen", "Omsk", "Chelyabinsk", "Saratov", "Yaroslavl", "Tula"],
    "jordan":             ["Irbid", "Zarqa", "Salt", "Fuheis", "Umm Qais", "Mafraq", "Tafilah", "Wadi Musa"],
    "qatar":              ["West Bay", "Al Rayyan", "Al Sadd", "Msheireb", "Umm Salal", "Al Daayen", "Al Ruwais", "Education City"],
    "oman":               ["Sohar", "Seeb", "Barka", "Ibri", "Rustaq", "Khasab", "Duqm", "Ibra"],
    "nepal":              ["Dharan", "Biratnagar", "Butwal", "Hetauda", "Janakpur", "Nepalgunj", "Gorkha", "Tansen", "Ilam", "Dhulikhel"],
    "bangladesh":         ["Barisal", "Mymensingh", "Bogra", "Jessore", "Dinajpur", "Narayanganj", "Gazipur", "Tangail", "Kushtia", "Feni", "Saidpur", "Pabna"],
    "estonia":            ["Voru", "Valga", "Paide", "Johvi", "Maardu", "Keila", "Sillamae", "Kohtla-Jarve"],
    "north_macedonia":    ["Gostivar", "Stip", "Kavadarci", "Gevgelija", "Struga", "Kicevo", "Krusevo", "Debar"],
    "brunei":             ["Gadong", "Kiulap", "Sengkurong", "Mentiri", "Kampong Ayer", "Berakas", "Rimba", "Panaga"],
    "bhutan":             ["Phuentsholing", "Gelephu", "Samdrup Jongkhar", "Samtse", "Lhuentse", "Zhemgang", "Trashiyangtse", "Dagana"],
    "belize":             ["Belmopan", "Benque Viejo del Carmen", "Sarteneja", "Maya Beach", "Seine Bight", "Ladyville", "Burrell Boom", "Crooked Tree"],
    # --- r2 wave 2: LATAM (mexico intentionally absent: its r2 already generated above) ---
    "colombia":           ["Cucuta", "Ibague", "Villavicencio", "Pasto", "Neiva", "Monteria", "Valledupar", "Armenia", "Popayan", "Tunja", "Sincelejo", "Riohacha", "Florencia", "Quibdo", "Leticia", "Yopal", "Barrancabermeja", "Sogamoso", "Duitama", "Girardot", "Zipaquira", "Guatape", "Salento", "Villa de Leyva", "Jardin", "Filandia", "Mompox", "Rincon del Mar", "Taganga", "Palomino", "Providencia", "Guatavita", "Buga", "Honda", "Jerico", "Nuqui", "Bahia Solano", "Capurgana", "El Penol", "Suesca"],
    "brazil":             ["Porto Alegre", "Belem", "Goiania", "Campinas", "Natal", "Maceio", "Joao Pessoa", "Vitoria", "Santos", "Paraty", "Ilhabela", "Buzios", "Gramado", "Campos do Jordao", "Ouro Preto", "Bonito", "Foz do Iguacu", "Fernando de Noronha", "Chapada Diamantina", "Lencois Maranhenses", "Trancoso", "Arraial do Cabo", "Jericoacoara", "Olinda", "Petropolis", "Tiradentes", "Sao Luis", "Porto de Galinhas", "Guaruja", "Ubatuba", "Angra dos Reis", "Alter do Chao", "Chapada dos Veadeiros", "Monte Verde", "Praia do Forte", "Itacare", "Morro de Sao Paulo", "Pipa", "Cunha", "Sao Sebastiao"],
    "argentina":          ["Tucuman", "Cafayate", "Purmamarca", "Tilcara", "El Chalten", "San Martin de los Andes", "Villa La Angostura", "La Plata", "Jujuy", "Neuquen", "Puerto Madryn", "Tigre", "Pinamar", "Colonia del Sacramento", "San Rafael"],
    "peru":               ["Chiclayo", "Piura", "Tacna", "Cajamarca", "Tarapoto", "Chachapoyas", "Paracas", "Ica", "Huancayo", "Pucallpa", "Puerto Maldonado", "Ollantaytambo"],
    "chile":              ["Antofagasta", "Puerto Varas", "Puerto Montt", "Valdivia", "Punta Arenas", "Arica", "Rancagua", "Talca", "Chillan", "Osorno", "Castro", "Coyhaique"],
    "bolivia":            ["El Alto", "Rurrenabaque", "Samaipata", "Villamontes", "Riberalta", "Camiri", "Montero", "Yacuiba", "Coroico", "Sorata"],
    "guatemala":          ["Coban", "Huehuetenango", "Retalhuleu", "Escuintla", "Mazatenango", "Puerto Barrios", "San Pedro La Laguna", "San Marcos La Laguna", "Jalapa", "Solola", "Chimaltenango", "Monterrico"],
    "panama":             ["Chitre", "Santiago de Veraguas", "Penonome", "Las Tablas", "Volcan", "Coronado", "Aguadulce", "La Chorrera", "Changuinola", "Isla Contadora"],
    "el_salvador":        ["Sonsonate", "Usulutan", "Ahuachapan", "Chalatenango", "San Vicente", "Zacatecoluca", "El Zonte", "La Palma", "Perquin", "Cojutepeque"],
    "costa_rica":         ["Heredia", "Alajuela", "Cartago", "Quepos", "Nosara", "Samara", "Uvita", "Puntarenas", "Turrialba", "Golfito", "Playa Flamingo", "Playas del Coco"],
    "paraguay":           ["San Lorenzo", "Fernando de la Mora", "Lambare", "Capiata", "Coronel Oviedo", "Caaguazu", "Itaugua", "Paraguari", "Pilar", "Ypacarai"],
    "nicaragua":          ["Jinotega", "Rivas", "Chinandega", "Juigalpa", "Somoto", "Ocotal", "Boaco", "Diriamba", "Jinotepe", "Nandaime"],
    },
    # r3: fresh tertiary cities — none repeat that country's r2 list above or the generator's round-1 cities[0:10]
    "r3": {
    "ecuador":     ["Santo Domingo", "Portoviejo", "Duran", "Milagro", "Quevedo", "Babahoyo", "Sangolqui", "Guaranda", "Azogues", "Macas", "Zamora", "Tulcan", "Cayambe", "Jipijapa", "Chone", "Bahia de Caraquez", "Gualaquiza", "Puyango"],
    "germany":     ["Aachen", "Kiel", "Lubeck", "Rostock", "Erfurt", "Wurzburg", "Bamberg", "Trier", "Koblenz", "Ulm", "Braunschweig", "Kassel", "Dortmund", "Essen", "Mannheim", "Potsdam", "Gottingen", "Bayreuth"],
    "india":       ["Chennai", "Bangalore", "Chandigarh", "Lucknow", "Indore", "Bhopal", "Nagpur", "Surat", "Coimbatore", "Madurai", "Jaisalmer", "Pushkar", "Manali", "Dharamshala", "Leh", "Hampi", "Alleppey", "Varkala"],
    "indonesia":   ["Sanur", "Uluwatu", "Nusa Dua", "Nusa Penida", "Kuta Lombok", "Senggigi", "Palembang", "Pekanbaru", "Pontianak", "Samarinda", "Denpasar", "Batu", "Cirebon", "Bukittinggi", "Lake Toba", "Ternate", "Ambon", "Kupang"],
    "italy":       ["Lucca", "Ravenna", "Como", "Matera", "Taormina", "Positano", "Ancona", "Cagliari", "Brescia", "Ferrara", "Mantua", "Vicenza", "Udine", "Trento", "Bolzano", "Salerno", "La Spezia", "Messina"],
    "japan":       ["Kagoshima", "Okayama", "Kurashiki", "Takamatsu", "Kochi", "Tokushima", "Oita", "Miyazaki", "Niigata", "Toyama", "Nagano", "Shizuoka", "Hamamatsu", "Gifu", "Utsunomiya", "Akita", "Aomori", "Morioka"],
    "philippines": ["Puerto Princesa", "Angeles City", "Subic Bay", "Batangas City", "Naga", "Tacloban", "Butuan", "General Santos", "Ormoc", "Roxas City", "Laoag", "Tuguegarao", "Malapascua", "Moalboal", "Oslob", "Camiguin", "Puerto Galera", "Banaue"],
    "portugal":    ["Albufeira", "Portimao", "Vilamoura", "Ericeira", "Peniche", "Figueira da Foz", "Leiria", "Covilha", "Braganca", "Vila Real", "Guarda", "Beja", "Elvas", "Sesimbra", "Espinho", "Matosinhos", "Vila Nova de Gaia", "Angra do Heroismo"],
    "spain":       ["Segovia", "Ronda", "Marbella", "Tenerife", "Las Palmas", "Santiago de Compostela", "Burgos", "Leon", "Oviedo", "A Coruna", "Almeria", "Huelva", "Jerez de la Frontera", "Cuenca", "Caceres", "Avila", "Lleida", "Castellon de la Plana"],
    "thailand":    ["Khon Kaen", "Nakhon Ratchasima", "Ubon Ratchathani", "Phitsanulok", "Nakhon Si Thammarat", "Rayong", "Pattaya", "Cha-am", "Prachuap Khiri Khan", "Ranong", "Satun", "Buriram", "Nong Khai", "Sakon Nakhon", "Phrae", "Phayao", "Kamphaeng Phet", "Ratchaburi"],
    "turkey":      ["Adana", "Mersin", "Kayseri", "Samsun", "Denizli", "Diyarbakir", "Erzurum", "Van", "Kutahya", "Afyonkarahisar", "Bolu", "Rize", "Ordu", "Sinop", "Amasya", "Safranbolu", "Datca", "Kalkan"],
    "vietnam":     ["Cat Ba", "Mai Chau", "Dong Hoi", "Phong Nha", "Kon Tum", "Tuy Hoa", "Phan Thiet", "Rach Gia", "Ca Mau", "My Tho", "Ben Tre", "Chau Doc", "Long Xuyen", "Vinh", "Thanh Hoa", "Nam Dinh", "Cao Bang", "Lang Son"],
    },
    # r4: deep reserve tier 1 — fresh quaternary cities for the high-city-count rotation countries.
    # No overlap with that country's r2, r3, or generator round-1 cities[0:10]. Auto-fuel advances into
    # r4 once r2/r3 files are consumed (NEW_CITIES = NEW_CITIES_BY_ROUND.get(ROUND, {}) handles it — no code change).
    "r4": {
    "germany":     ["Saarbrucken", "Wolfsburg", "Ingolstadt", "Oldenburg", "Osnabruck", "Paderborn", "Heilbronn", "Pforzheim", "Gelsenkirchen", "Wuppertal", "Bochum", "Halle", "Magdeburg", "Chemnitz", "Jena", "Konstanz", "Duisburg", "Krefeld"],
    "india":       ["Gangtok", "Nainital", "Ooty", "Mount Abu", "Bikaner", "Ranthambore", "Khajuraho", "Kodaikanal", "Coorg", "Wayanad", "Hampi Bazaar", "Kasol", "Spiti", "Tirupati", "Madikeri", "Gokarna", "Mahabalipuram", "Bhubaneswar"],
    "indonesia":   ["Gili Air", "Amed", "Lovina", "Munduk", "Sidemen", "Wakatobi", "Jimbaran", "Nusa Lembongan", "Tanjung Benoa", "Sumbawa", "Belitung", "Karimunjawa", "Derawan", "Bunaken", "Tomohon", "Wonosobo", "Probolinggo", "Jepara"],
    "italy":       ["Cremona", "Pavia", "Alghero", "Sassari", "Viterbo", "Assisi", "Orvieto", "Gubbio", "Spoleto", "Cefalu", "Syracuse", "Ragusa", "Noto", "Tropea", "Portofino", "Rapallo", "Sirmione", "Merano"],
    "japan":       ["Fukushima", "Yamagata", "Wakayama", "Naha", "Ishigaki", "Miyakojima", "Karuizawa", "Ise", "Nagaoka", "Iwaki", "Yonago", "Tottori", "Fukui", "Mito", "Chiba", "Kofu", "Nagasaki Bay", "Onomichi"],
    "philippines": ["Siquijor", "Dapitan", "Dipolog", "Bantayan", "Panglao", "Anilao", "Calaguas", "Bataan", "Guimaras", "Kalibo", "San Juan", "Sabang", "Port Barton", "Balabac", "Apo Island", "Donsol", "Sipalay", "Alona Beach"],
    "portugal":    ["Sagres", "Loule", "Portalegre", "Castelo Branco", "Chaves", "Lamego", "Monsaraz", "Marvao", "Estremoz", "Silves", "Caldas da Rainha", "Fatima", "Alcobaca", "Batalha", "Mira", "Ovar", "Amarante", "Penafiel"],
    "spain":       ["Zamora", "Soria", "Palencia", "Teruel", "Albacete", "Ourense", "Jaen", "Ciudad Real", "Guadalajara", "Badajoz", "Lugo", "Pontevedra", "Melilla", "Ceuta", "Aranjuez", "Sitges", "Cangas de Onis", "Nerja"],
    "thailand":    ["Sing Buri", "Ang Thong", "Chachoengsao", "Nakhon Pathom", "Suphan Buri", "Uttaradit", "Tak", "Mae Sot", "Betong", "Krabi Town", "Chanthaburi", "Trat", "Loei", "Chaiyaphum", "Roi Et", "Sisaket", "Yasothon", "Kalasin"],
    "turkey":      ["Sivas", "Malatya", "Nevsehir", "Aksaray", "Kastamonu", "Tokat", "Nigde", "Corum", "Kirsehir", "Yozgat", "Usak", "Isparta", "Burdur", "Aydin", "Manisa", "Balikesir", "Tekirdag", "Edirne"],
    "vietnam":     ["Bac Ha", "Moc Chau", "Con Dao", "Hoa Lu", "Tam Coc", "Sam Son", "Cua Lo", "Dien Bien Phu", "Ninh Binh City", "Ha Tien", "Chau Doc Market", "Sa Dec", "Vinh Long", "Tra Vinh", "Bao Loc", "Gia Nghia", "Tam Ky", "Dong Ha"],
    "egypt":       ["Damietta", "Minya", "Beni Suef", "Qena", "Zagazig", "Kom Ombo", "Edfu", "Rosetta", "Taba", "Ras Sedr", "Safaga", "Quseir", "Assiut", "Bawiti", "Farafra", "Marsa Matruh", "El Alamein", "Ain Sokhna"],
    "morocco":     ["Nador", "Oujda", "Beni Mellal", "Taroudant", "Zagora", "Tinghir", "Midelt", "Larache", "Kenitra", "Khenifra", "Errachidia", "Sidi Ifni", "Mirleft", "Imlil", "Ait Benhaddou", "Skoura", "Tafraoute", "Moulay Idriss"],
    "mexico":      ["Puebla", "Durango", "Chihuahua", "Colima", "Tepoztlan", "Tequila", "Cholula", "Izamal", "Palenque", "Chetumal", "Loreto", "Ensenada", "Cuetzalan", "Real de Catorce", "Huatulco", "Zihuatanejo", "Ixtapa", "Cozumel"],
    "colombia":    ["Minca", "San Gil", "Barichara", "Cocora", "Nabusimake", "Tolu", "Melgar", "Ubate", "Chia", "Cajica", "Pance", "Guaduas", "Ocana", "Girardota", "Andres", "Chinacota", "Aguachica", "Ciudad Bolivar"],
    "brazil":      ["Torres", "Garopaba", "Bombinhas", "Canela", "Balneario Camboriu", "Maragogi", "Canoa Quebrada", "Sao Miguel dos Milagres", "Barra Grande", "Aracaju", "Cuiaba", "Campo Grande", "Teresina", "Joinville", "Blumenau", "Diamantina", "Capitolio", "Brotas"],
    "argentina":   ["Villa General Belgrano", "Cachi", "Humahuaca", "Chos Malal", "Las Grutas", "Villa Gesell", "Necochea", "Tandil", "Concordia", "Gualeguaychu", "Trelew", "Villa Carlos Paz", "Merlo", "Esquel", "Rio Gallegos", "Posadas", "Corrientes", "Santa Fe"],
    "china":       ["Chengde", "Datong", "Pingyao", "Luoyang", "Zhengzhou", "Jinan", "Wuxi", "Yantai", "Weihai", "Jiuzhaigou", "Emeishan", "Zhangye", "Dunhuang", "Turpan", "Kashgar", "Zhouzhuang", "Wuzhen", "Tongli"],
    "russia":      ["Suzdal", "Vladimir", "Pskov", "Veliky Novgorod", "Murmansk", "Vologda", "Kostroma", "Ryazan", "Kaluga", "Petrozavodsk", "Arkhangelsk", "Ulan-Ude", "Khabarovsk", "Astrakhan", "Tomsk", "Barnaul", "Kemerovo", "Smolensk"],
    "south_korea": ["Boseong", "Damyang", "Gyeongsan", "Yangyang", "Gapyeong", "Namhae", "Geoje", "Hongseong", "Buyeo", "Gongju", "Gunsan", "Jecheon", "Wonju", "Uljin", "Yeongju", "Mungyeong", "Hadong", "Goseong"],
    },
    # r5: deep reserve tier 2 — further fresh cities. No overlap with that country's r2, r3, r4, or
    # generator round-1 cities[0:10]. Exhausting r5 with zero files means the country's defined pools are
    # spent and the autofuel script should alert to author r6+ cities.
    "r5": {
    "germany":     ["Passau", "Lindau", "Fussen", "Baden-Baden", "Tubingen", "Marburg", "Schwerin", "Flensburg", "Gorlitz", "Quedlinburg", "Goslar", "Wismar", "Stralsund", "Meissen", "Weimar", "Bremerhaven", "Cottbus", "Zwickau"],
    "india":       ["Kanyakumari", "Rameswaram", "Tirunelveli", "Vellore", "Thanjavur", "Ajmer", "Chittorgarh", "Bundi", "Mandawa", "Orchha", "Gwalior", "Nashik", "Aurangabad", "Kolhapur", "Vadodara", "Rajkot", "Dehradun", "Mussoorie"],
    "indonesia":   ["Wonosari", "Magelang", "Salatiga", "Pematangsiantar", "Palu", "Kendari", "Gorontalo", "Jayapura", "Sorong", "Manokwari", "Mataram", "Praya", "Ende", "Maumere", "Ruteng", "Bajawa", "Waingapu", "Atambua"],
    "italy":       ["Ragusa Ibla", "Agrigento", "Trapani", "Marsala", "Enna", "Caltanissetta", "Reggio Calabria", "Cosenza", "Catanzaro", "Crotone", "Potenza", "Campobasso", "Isernia", "Teramo", "Pescara", "Chieti", "L'Aquila", "Frosinone"],
    "japan":       ["Kurume", "Saga", "Sasebo", "Nobeoka", "Kirishima", "Amami", "Shimonoseki", "Iwakuni", "Tsuwano", "Hagi", "Izumo", "Matsue", "Kurayoshi", "Maizuru", "Toba", "Suzuka", "Owase", "Shingu"],
    "philippines": ["Batanes", "Basco", "Currimao", "Pagudpud", "San Fernando La Union", "Baler", "Infanta", "Real", "Mati", "Surigao", "Butuan City", "Ozamiz", "Pagadian", "Cotabato", "Marbel", "Kidapawan", "Digos", "Tandag"],
    "portugal":    ["Vila do Conde", "Povoa de Varzim", "Barcelos", "Ponte de Lima", "Melgaco", "Miranda do Douro", "Almeida", "Sabugal", "Serpa", "Moura", "Mertola", "Odemira", "Aljezur", "Vila Real de Santo Antonio", "Grandola", "Alcacer do Sal", "Palmela", "Torres Vedras"],
    "spain":       ["Vitoria-Gasteiz", "Huesca", "Ferrol", "Manresa", "Vic", "Figueres", "Cadaques", "Mojacar", "Ubeda", "Baeza", "Antequera", "Estepona", "Fuengirola", "Torremolinos", "Denia", "Javea", "Calpe", "Peniscola"],
    "thailand":    ["Nakhon Sawan", "Uthai Thani", "Kanchanaburi Town", "Saraburi", "Nakhon Nayok", "Prachin Buri", "Sa Kaeo", "Samut Songkhram", "Samut Sakhon", "Phatthalung", "Yala", "Narathiwat", "Pattani", "Phang Nga", "Krabi Beach", "Phanom Rung", "Mukdahan", "Nakhon Phanom"],
    "turkey":      ["Sapanca", "Abant", "Uzungol", "Amasra", "Bartin", "Zonguldak", "Duzce", "Sakarya", "Kocaeli", "Yalova", "Bilecik", "Kutahya Town", "Corum City", "Cankiri", "Karabuk", "Bayburt", "Gumushane", "Artvin"],
    "vietnam":     ["Ninh Thuan Beach", "Bao Loc Falls", "Buon Ho", "Dak Nong", "Kbang", "An Khe", "Song Cau", "La Gi", "Ham Tan", "Cam Ranh", "Dien Khanh", "Thap Cham", "Phan Rang", "Duc Trong", "Di Linh", "Lam Ha", "Cat Tien", "Gia Lai"],
    "egypt":       ["Baltim", "Damanhur", "Kafr El Sheikh", "Abydos", "Dendera", "Rashid", "Idfu", "Esna", "Armant", "Nag Hammadi", "Girga", "Akhmim", "Bilbeis", "Banha", "Shibin El Kom", "Desouk", "Kafr El Dawwar", "Talkha"],
    "morocco":     ["Ouzoud", "Fnideq", "Al Hoceima", "Safi", "Settat", "Berkane", "Guelmim", "Tan-Tan", "Laayoune", "Smara", "Ksar el-Kebir", "Sefrou", "Taza", "Khemisset", "Sidi Kacem", "Ben Guerir", "Youssoufia", "Berrechid"],
    "mexico":      ["Tepic", "Saltillo", "Hermosillo", "Ciudad Victoria", "Tlaxcala", "Comitan", "Tapachula", "Valle de Bravo", "Malinalco", "Bernal", "Mineral del Chico", "Creel", "Batopilas", "Alamos", "Patzcuaro", "Uruapan", "Tequisquiapan", "Xilitla"],
    "colombia":    ["Amalfi", "Sonson", "Aguadas", "Salamina", "Concepcion", "Envigado", "Sabaneta", "La Ceja", "Retiro", "Marinilla", "Rionegro", "Carmen de Viboral", "Santa Fe de Antioquia", "Guarne", "Tamesis", "Fredonia", "Concordia Antioquia", "Betulia"],
    "brazil":      ["Socorro", "Serra Negra", "Aguas de Lindoia", "Holambra", "Sao Roque", "Atibaia", "Ibitipoca", "Conservatoria", "Penedo", "Visconde de Maua", "Sana", "Nova Friburgo", "Teresopolis", "Domingos Martins", "Venda Nova", "Pomerode", "Nova Petropolis", "Bento Goncalves"],
    "argentina":   ["Reconquista", "Villa Union", "Chilecito", "Belen", "Andalgala", "Tafi del Valle", "Amaicha del Valle", "Molinos", "Angastaco", "Seclantas", "El Bolson", "Lago Puelo", "Cholila", "Trevelin", "Gaiman", "Rawson", "Viedma", "Carmen de Patagones"],
    "china":       ["Yangzhou", "Zhenjiang", "Nantong", "Changzhou", "Xuzhou", "Fuzhou", "Quanzhou", "Zhuhai", "Foshan", "Zhaoqing", "Yichang", "Enshi", "Fenghuang", "Kaili", "Anshun", "Libo", "Xingyi", "Beihai"],
    "russia":      ["Cheboksary", "Izhevsk", "Kirov", "Penza", "Orenburg", "Tolyatti", "Ivanovo", "Bryansk", "Kursk", "Belgorod", "Lipetsk", "Tambov", "Vladikavkaz", "Nalchik", "Pyatigorsk", "Grozny", "Makhachkala", "Cherepovets"],
    "south_korea": ["Sacheon", "Miryang", "Yeongcheon", "Sangju", "Gimcheon", "Yeongju City", "Yeongyang", "Bonghwa", "Uljin Beach", "Yeonggwang", "Wando", "Jindo", "Haenam", "Gangjin", "Jangheung", "Boryeong", "Seosan", "Taean"],
    },
}

NEW_CITIES = NEW_CITIES_BY_ROUND.get(ROUND, {})
if not NEW_CITIES:
    print(f"no city lists defined for round '{ROUND}' (known: {', '.join(sorted(NEW_CITIES_BY_ROUND.keys()))})")

total_files = 0; total_kw = 0; total_skipped = 0
for country, cities in NEW_CITIES.items():
    if ONLY_COUNTRIES and country not in ONLY_COUNTRIES:
        continue
    cdir = os.path.join(BASE, country)
    if not os.path.isdir(cdir):
        print(f"  skip {country}: no dir"); continue
    c_files = 0; c_skipped = 0
    for cat, terms in HIGH_YIELD.items():
        if ONLY_CATS and cat not in ONLY_CATS:
            continue
        kws = [f"{t} near {c} {country.replace('_',' ').title()}" for c in cities for t in terms]
        random.shuffle(kws)
        # chunk into ~24-keyword files
        for i in range(0, len(kws), 24):
            chunk = kws[i:i+24]
            n = i//24 + 1
            name = f"{cat}_{ROUND}_{n}.txt"
            # collision safety: never overwrite active, done or quarantined work
            existing = None
            for sub in ("", "done", "quarantine"):
                cand = os.path.join(cdir, sub, name) if sub else os.path.join(cdir, name)
                if os.path.exists(cand):
                    existing = cand; break
            if existing is not None:
                print(f"  WARN skip {country}\\{name}: already exists at {existing}")
                c_skipped += 1; total_skipped += 1
                continue
            fn = os.path.join(cdir, name)
            with open(fn, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk) + "\n")
            c_files += 1; total_files += 1; total_kw += len(chunk)
    print(f"  {country}: {len(cities)} new cities, {c_files} files written, {c_skipped} skipped")

print(f"\nDONE [{ROUND}]: {total_files} new keyword files, {total_kw} keywords, {total_skipped} skipped across {len(NEW_CITIES)} countries")
print(f"at ~46 files/day that's ~{total_files//46} days of fresh fuel")
