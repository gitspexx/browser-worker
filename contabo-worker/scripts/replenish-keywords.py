#!/usr/bin/env python3
"""
Replenish botsol keyword fuel WITHOUT the duplicate trap.
Round-1 already scraped the high-yield terms across each country's original ~10 cities,
so re-running term x same-city = pure dup. Fresh, non-dup fuel = high-yield terms x NEW
secondary cities (not yet scraped). Writes chunked ~24-keyword files into
keywords_v2/<country>/<cat>_r2_<n>.txt so the agent picks them up as fresh active work.
"""
import os, random
random.seed(42)
BASE = r"C:\Botsol\pipeline\keywords_v2"

# Proven high-yield terms (from the pruned round-1 analysis) — the keepers only.
HIGH_YIELD = {
    "eat":      ["restaurant", "seafood restaurant", "steakhouse", "local food", "traditional restaurant", "fine dining", "street food"],
    "cafe":     ["coffee shop", "cafe", "specialty coffee", "bakery cafe", "brunch cafe", "dessert cafe"],
    "drink":    ["bar", "cocktail bar", "rooftop bar", "wine bar", "brewery", "pub", "nightclub"],
    "stay":     ["hotel", "boutique hotel", "hostel", "resort", "guest house", "bed and breakfast"],
    "explore":  ["tour", "city tour", "walking tour", "food tour", "boat tour", "day trip", "sightseeing tour"],
    "do":       ["adventure tour", "cooking class", "museum", "cultural tour", "excursion", "art gallery"],
    "wellness": ["spa", "yoga studio", "massage", "wellness center", "thermal bath"],
}

# NEW secondary cities (beyond the ~10 already scraped in round-1). Country dir name -> cities.
NEW_CITIES = {
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
}

total_files = 0; total_kw = 0
for country, cities in NEW_CITIES.items():
    cdir = os.path.join(BASE, country)
    if not os.path.isdir(cdir):
        print(f"  skip {country}: no dir"); continue
    for cat, terms in HIGH_YIELD.items():
        kws = [f"{t} near {c} {country.replace('_',' ').title()}" for c in cities for t in terms]
        random.shuffle(kws)
        # chunk into ~24-keyword files
        for i in range(0, len(kws), 24):
            chunk = kws[i:i+24]
            n = i//24 + 1
            fn = os.path.join(cdir, f"{cat}_r2_{n}.txt")
            with open(fn, "w", encoding="utf-8") as f:
                f.write("\n".join(chunk) + "\n")
            total_files += 1; total_kw += len(chunk)
    print(f"  {country}: {len(cities)} new cities")

print(f"\nDONE: {total_files} new keyword files, {total_kw} keywords across {len(NEW_CITIES)} countries")
print(f"at ~46 files/day that's ~{total_files//46} days of fresh fuel")
