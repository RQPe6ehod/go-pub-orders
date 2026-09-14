// GO pub — order board backend.
// One Durable Object instance ("main") holds:
//  - orders: active kitchen/bar tickets (accept -> ready -> served)
//  - openTables: tables currently occupied (a "tab"), independent of
//    individual ticket status — stays open across multiple rounds until
//    the waiter explicitly closes the table
//  - history: served order tickets (used to reconstruct a table's full bill)
//  - closedTables: finalized receipts for closed tables
// Everything is broadcast to every connected client (waiter / cook /
// bartender / manager) over WebSocket. Each role must present the
// matching PIN (set in wrangler.toml [vars]) before it can send anything.

const HISTORY_LIMIT = 500;
const CLOSED_TABLES_LIMIT = 300;
const SERVICE_RATE = 0.10;

function parsePrice(p) {
  if (typeof p === "number") return p;
  if (!p) return 0;
  const n = parseInt(String(p).replace(/[^\d]/g, ""), 10);
  return isNaN(n) ? 0 : n;
}

function lineTotal(items) {
  return (items || []).reduce((sum, i) => sum + parsePrice(i.price) * (i.qty || 1), 0);
}

const DEFAULT_MENU = {"bar":[{"title":{"ru":"Бутылочное пиво","en":"Bottled Beer","kk":"Бөтелкедегі сыра"},"items":[{"name":{"ru":"Heineken 0.5","en":"Heineken 0.5","kk":"Heineken 0.5"},"price":"2 250","id":"dbebb8be-f013-43b4-b706-a0c4f0fc4a95"},{"name":{"ru":"Heineken безалкогольное 0.5","en":"Heineken Non-Alcoholic 0.5","kk":"Heineken алкогольсіз 0.5"},"price":"2 450","id":"70f85f0d-0971-48aa-839f-8ea2b977bbf0"},{"name":{"ru":"Kronenbourg 1664 0.5L","en":"Kronenbourg 1664 0.5L","kk":"Kronenbourg 1664 0.5L"},"price":"2 350","id":"9f137830-813d-4f7e-85ed-eff38bf3ecc3"},{"name":{"ru":"Corona Extra 0.35L","en":"Corona Extra 0.35L","kk":"Corona Extra 0.35L"},"price":"3 550","id":"a5e9bbaa-3626-4f28-ac5e-172a0e3a2fd2"},{"name":{"ru":"Krusovice Svetle 0.45L","en":"Krusovice Svetle 0.45L","kk":"Krusovice Svetle 0.45L"},"price":"1 590","id":"2aca41a9-87b0-442f-9947-8c22ad9719c2"},{"name":{"ru":"Krusovice Cerne 0.45L","en":"Krusovice Cerne 0.45L","kk":"Krusovice Cerne 0.45L"},"price":"1 590","id":"7d9174cc-c678-40fd-9130-63b8c22339de"},{"name":{"ru":"Жигули Барное безалкогольное 0.45L","en":"Zhiguli Barnoye Non-Alcoholic 0.45L","kk":"Жигули Барное алкогольсіз 0.45L"},"price":"1 450","id":"084c76b7-6726-4ec5-b6aa-f95a8a21207e"}],"id":"4944d2f8-0d75-4187-ab00-899dcf0d7460"},{"title":{"ru":"Разливное пиво","en":"Draft Beer","kk":"Құйма сыра"},"note":{"ru":"цена за 0.5 л / за 3 л","en":"price per 0.5 L / 3 L","kk":"0.5 л / 3 л бағасы"},"items":[{"name":{"ru":"Пражское","en":"Prague","kk":"Прага"},"price":"1 450","price2":"7 990","id":"daddf3cb-267d-4aee-8b53-c83bb92d273a"},{"name":{"ru":"Баварское нефильтрованное","en":"Bavarian Unfiltered","kk":"Бавария сүзілмеген"},"price":"1 750","price2":"9 990","id":"dd7c1246-1315-429e-9dd1-5a39ad92fd35"},{"name":{"ru":"Budweiser Budvar","en":"Budweiser Budvar","kk":"Budweiser Budvar"},"price":"3 590","price2":"20 590","id":"ff5b8fed-13f2-47f8-aeb1-26e97e6d6c73"},{"name":{"ru":"Guinness","en":"Guinness","kk":"Guinness"},"price":"3 990","id":"e9e4077a-bf99-454e-a875-86d0aa897105"}],"id":"736c1525-3f6f-45c8-aec0-9eb28c111a2f"},{"title":{"ru":"Закуски к пиву","en":"Beer Snacks","kk":"Сыраға тіскебасар"},"items":[{"name":{"ru":"Арахис соленый","en":"Salted Peanuts","kk":"Тұзды жержаңғақ"},"price":"1 690","id":"0f519828-9351-4b7e-827c-0176a890c1c5"},{"name":{"ru":"Чечил","en":"Chechil Cheese","kk":"Шешіл ірімшігі"},"price":"1 990","id":"c0c4a079-886a-4930-9b06-e5e23f804cd4"},{"name":{"ru":"Чипсы","en":"Chips","kk":"Чипсы"},"price":"2 190","id":"8dafb8db-5ccc-42eb-9259-62be0b4e080e"},{"name":{"ru":"Фисташки соленые","en":"Salted Pistachios","kk":"Тұзды фисташка"},"price":"2 390","id":"d09b080c-d422-4049-a7f1-3e6359f21ade"},{"name":{"ru":"Курт","en":"Kurt","kk":"Құрт"},"price":"1 990","id":"3ac66536-9aec-4bd2-b614-54d99333feb8"}],"id":"54517935-df6c-4fdf-bb8d-450dee13c4f3"},{"title":{"ru":"Водка","en":"Vodka","kk":"Арақ"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Хортица","en":"Khortytsa","kk":"Хортица"},"price":"9 500","price2":"950","id":"cdd52d12-eb20-4e12-b2be-ccfb4a9f8555"},{"name":{"ru":"Бульбашъ особая","en":"Bulbash Osobaya","kk":"Бульбашъ особая"},"price":"9 800","price2":"980","id":"a2fe89b9-6aaa-484e-88d3-b8e6a050cb69"},{"name":{"ru":"Kyzyl Zhar Legend","en":"Kyzyl Zhar Legend","kk":"Kyzyl Zhar Legend"},"price":"9 900","price2":"990","id":"59bbbea7-7f0b-4fdc-aabb-3ea6da8fdb0e"},{"name":{"ru":"Romanov","en":"Romanov","kk":"Romanov"},"price":"11 500","price2":"1 150","id":"786f95ca-f2de-4733-8a39-b8256afc8941"},{"name":{"ru":"Tchaikovsky","en":"Tchaikovsky","kk":"Tchaikovsky"},"price":"12 900","price2":"1 290","id":"c8ac0587-f19a-403f-afca-856624a07902"},{"name":{"ru":"Smirnoff №21 RED","en":"Smirnoff №21 RED","kk":"Smirnoff №21 RED"},"price":"13 900","price2":"1 390","id":"eaa5a986-d9e2-4a6e-8366-e7775211e6c8"},{"name":{"ru":"Absolut Blue","en":"Absolut Blue","kk":"Absolut Blue"},"price":"19 500","price2":"1 950","id":"7a60935c-b76a-4cbc-87d4-f33cfe37f268"},{"name":{"ru":"Tito's","en":"Tito's","kk":"Tito's"},"price":"27 900","price2":"2 790","id":"5348b762-0d4e-459b-be77-b1fcde85308a"},{"name":{"ru":"Reyka","en":"Reyka","kk":"Reyka"},"price":"32 000","price2":"3 200","id":"8d763dd7-b3e4-4cd7-b7fa-48661dc276f3"},{"name":{"ru":"Grey Goose","en":"Grey Goose","kk":"Grey Goose"},"price":"37 000","price2":"3 700","id":"75eb9fb3-3e7e-4adc-9208-49fc3185a17b"},{"name":{"ru":"Belvedere","en":"Belvedere","kk":"Belvedere"},"price":"41 000","price2":"4 100","id":"a3a14860-9119-4085-8c6e-8b48bb3f5289"}],"id":"6d8c237c-9ef6-4867-ac3e-178c8768fca7"},{"title":{"ru":"Виски","en":"Whisky","kk":"Виски"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Ballantine's","en":"Ballantine's","kk":"Ballantine's"},"price":"23 500","price2":"2 350","id":"b2e3c78b-7fdc-4d9b-a0d1-64c1d1cb4245"},{"name":{"ru":"Red Label","en":"Red Label","kk":"Red Label"},"price":"24 500","price2":"2 450","id":"cdade56d-b2fd-44fc-b1d9-6be8cd9da696"},{"name":{"ru":"Jameson","en":"Jameson","kk":"Jameson"},"price":"26 000","price2":"2 600","id":"93c19a2f-9967-4a8c-bc24-fa5652d3cf4b"},{"name":{"ru":"Tullamore D.E.W.","en":"Tullamore D.E.W.","kk":"Tullamore D.E.W."},"price":"28 000","price2":"2 800","id":"14989302-691d-462d-8978-c1ba6c1be536"},{"name":{"ru":"Jack Daniels","en":"Jack Daniels","kk":"Jack Daniels"},"price":"33 500","price2":"3 500","id":"ad5ce8e2-5f07-4e2d-a50b-f97e3d5afcca"},{"name":{"ru":"Monkey Shoulder","en":"Monkey Shoulder","kk":"Monkey Shoulder"},"price":"39 500","price2":"3 900","id":"cd893e80-294e-497a-a9e3-4d1756a04a0b"},{"name":{"ru":"Chivas 12 y.o.","en":"Chivas 12 y.o.","kk":"Chivas 12 y.o."},"price":"41 000","price2":"4 100","id":"ac532d86-f1a0-4e0a-bd14-26395b41fb78"}],"id":"ba74596a-db64-4426-83e4-195555c14652"},{"title":{"ru":"Коньяк","en":"Cognac","kk":"Коньяк"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Казахстан 3 звезды","en":"Kazakhstan 3 Stars","kk":"Қазақстан 3 жұлдыз"},"price":"12 500","price2":"1 250","id":"9195ca37-af37-46fa-81d1-18ecfe968db3"},{"name":{"ru":"Казахстан 5 звезд","en":"Kazakhstan 5 Stars","kk":"Қазақстан 5 жұлдыз"},"price":"15 500","price2":"1 500","id":"98e52f32-7aa1-4720-9e3f-1ddcfde413db"},{"name":{"ru":"Арарат 3 звезды","en":"Ararat 3 Stars","kk":"Арарат 3 жұлдыз"},"price":"17 500","price2":"1 750","id":"cdbb1063-680d-4bfd-b308-9ba3f4e03abd"},{"name":{"ru":"Арарат 5 звезд","en":"Ararat 5 Stars","kk":"Арарат 5 жұлдыз"},"price":"20 000","price2":"2 000","id":"df059af1-3b52-49ab-9a14-787a6c83717b"},{"name":{"ru":"Hennessy V.S.","en":"Hennessy V.S.","kk":"Hennessy V.S."},"price":"45 000","price2":"4 500","id":"0683cbd2-ffe8-40e4-984d-75eadae415bc"}],"id":"d23b2b8d-7f5a-4d86-85da-35c08af3a9bf"},{"title":{"ru":"Ром","en":"Rum","kk":"Ром"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Oakheart","en":"Oakheart","kk":"Oakheart"},"price":"20 000","price2":"2 000","id":"6dc68c6c-fe20-4013-acb6-799b6f1597be"}],"id":"e31d38c3-1d1f-489d-b4a7-9b2fc09842f1"},{"title":{"ru":"Текила","en":"Tequila","kk":"Текила"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Olmeca","en":"Olmeca","kk":"Olmeca"},"price":"21 500","price2":"2 150","id":"75021b5f-9bd4-40d3-a2b0-5bf0286aa43b"}],"id":"b653e02c-c9f0-44f7-8bbc-d3b7ef0eb7ee"},{"title":{"ru":"Джин","en":"Gin","kk":"Джин"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Gordon's","en":"Gordon's","kk":"Gordon's"},"price":"18 500","price2":"1 850","id":"26efc7d2-6be2-469e-8754-b032c16bda38"},{"name":{"ru":"Beefeater","en":"Beefeater","kk":"Beefeater"},"price":"19 500","price2":"1 950","id":"9550e3d7-cba9-44cf-829c-6b00ac7d0210"},{"name":{"ru":"Bickens","en":"Bickens","kk":"Bickens"},"price":"20 500","price2":"2 050","id":"0380412d-82aa-4376-9740-69a7c9596791"}],"id":"18ea54c7-2b54-433b-8ee3-d003f371ccfc"},{"title":{"ru":"Ликеры","en":"Liqueurs","kk":"Ликерлер"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Becherovka","en":"Becherovka","kk":"Becherovka"},"price":"18 500","price2":"1 850","id":"a811fecf-e411-47d4-b9f0-de51d759494f"},{"name":{"ru":"Baileys","en":"Baileys","kk":"Baileys"},"price":"19 500","price2":"1 950","id":"6018aa14-8e8b-4955-9a7a-ad0dfb1d316e"},{"name":{"ru":"Jägermeister","en":"Jägermeister","kk":"Jägermeister"},"price":"21 000","price2":"2 100","id":"c1a67470-bca3-447a-affb-bc10cf892535"}],"id":"e6e26a05-36b3-43b8-81d2-88af5a5cbbcd"},{"title":{"ru":"Аперитивы и вино","en":"Aperitifs & Wine","kk":"Аперитивтер мен шарап"},"items":[{"name":{"ru":"Martini","en":"Martini","kk":"Martini"},"price":"18 500","price2":"1 850","desc":{"ru":"1 л / 100 мл","en":"1 L / 100 ml","kk":"1 л / 100 мл"},"id":"8f5b10f4-a5fe-48bb-94db-809e0aef87e5"},{"name":{"ru":"Вино в ассортименте","en":"Assorted Wine","kk":"Түрлі шарап"},"price":"14 000","price2":"2 200","desc":{"ru":"0.7 л / 100 мл","en":"0.7 L / 100 ml","kk":"0.7 л / 100 мл"},"id":"9107506c-f6b0-40db-a0ca-3d408295bc66"}],"id":"1576be07-4858-4637-84bd-e0e0ee3f8d82"},{"title":{"ru":"Коктейли алкогольные","en":"Cocktails","kk":"Коктейльдер"},"items":[{"name":{"ru":"Aperol Spritz","en":"Aperol Spritz","kk":"Aperol Spritz"},"price":"3 190","id":"95c99a7e-54f9-4dde-bf06-75a11065c8d7"},{"name":{"ru":"Gin Tonic","en":"Gin Tonic","kk":"Gin Tonic"},"price":"2 790","id":"3bb61f9d-9c49-4f2b-95eb-719a2f509999"},{"name":{"ru":"Long Island","en":"Long Island","kk":"Long Island"},"price":"3 290","id":"41f29efe-8950-48e9-a085-620d2ecc3fbf"},{"name":{"ru":"Cuba Libre","en":"Cuba Libre","kk":"Cuba Libre"},"price":"2 590","id":"a5e2e81b-a6e5-40f7-b700-ad74ebdda6d3"},{"name":{"ru":"Mimosa","en":"Mimosa","kk":"Mimosa"},"price":"2 490","id":"a700f097-35fa-4d25-9478-63ff6c5887af"},{"name":{"ru":"Mojito","en":"Mojito","kk":"Mojito"},"price":"2 690","id":"761b1ab0-8ae4-422c-8003-7dcbec61a2c8"}],"id":"63dd85b7-83ae-419e-a2fb-dd722f0b650b"},{"title":{"ru":"Безалкогольные напитки","en":"Soft Drinks","kk":"Алкогольсіз сусындар"},"items":[{"name":{"ru":"Coca-Cola","en":"Coca-Cola","kk":"Coca-Cola"},"price":"1 990","price2":"1 490","price3":"1 690","desc":{"ru":"1 л / 0.5 л / 0.25 л","en":"1 L / 0.5 L / 0.25 L","kk":"1 л / 0.5 л / 0.25 л"},"id":"e9429f66-768a-4e71-a30d-352c1a0f51cd"},{"name":{"ru":"Минеральная вода","en":"Mineral Water","kk":"Минералды су"},"price":"1 150","price2":"890","desc":{"ru":"1 л / 0.5 л","en":"1 L / 0.5 L","kk":"1 л / 0.5 л"},"id":"a12f366a-439e-4e80-9d03-645bf044aaf4"},{"name":{"ru":"Сок","en":"Juice","kk":"Шырын"},"price":"2 390","id":"bf7cfd93-9870-427f-9d99-f95e9b994615"},{"name":{"ru":"Red Bull","en":"Red Bull","kk":"Red Bull"},"price":"2 450","id":"59603683-809e-418e-9b9d-f058bbf035bb"},{"name":{"ru":"Borjomi","en":"Borjomi","kk":"Borjomi"},"price":"2 550","id":"fe853949-445e-4abc-9690-0da9c90455ea"},{"name":{"ru":"Тоник Schweppes","en":"Schweppes Tonic","kk":"Schweppes тоник"},"price":"2 300","id":"856bc6d5-917b-4d16-977f-af48ea207cb0"},{"name":{"ru":"Лимонады","en":"Lemonades","kk":"Лимонадтар"},"price":"2 790","id":"20f28af7-99a9-4ea9-a8fd-b8f69072b7ae"},{"name":{"ru":"Морс клюквенный","en":"Cranberry Mors","kk":"Мүкжидек морсы"},"price":"2 590","id":"f298ff37-1ad2-49ff-b47f-cf16540509a8"}],"id":"c2d6c4fd-b06b-4f57-a4cd-a9fabab01442"},{"title":{"ru":"Чай","en":"Tea","kk":"Шай"},"items":[{"name":{"ru":"Чай в ассортименте (чашка)","en":"Assorted Tea (cup)","kk":"Түрлі шай (кесе)"},"price":"800","id":"95c4fe9a-91f4-4a2e-a0be-073442778f8a"},{"name":{"ru":"Чай черный (чайник)","en":"Black Tea (pot)","kk":"Қара шай (шәйнек)"},"price":"1 450","id":"abbeabe2-5bea-4768-88ba-a3b2581359e9"},{"name":{"ru":"Чай зеленый (чайник)","en":"Green Tea (pot)","kk":"Жасыл шай (шәйнек)"},"price":"1 650","id":"bdcecb78-c0c0-4487-885b-0b59499f3576"},{"name":{"ru":"Чай ташкентский (чайник)","en":"Tashkent Tea (pot)","kk":"Ташкент шайы (шәйнек)"},"price":"2 190","id":"7607d91f-14f1-4fb5-9e06-f65c2a7f4709"},{"name":{"ru":"Чай имбирный (чайник)","en":"Ginger Tea (pot)","kk":"Зімбір шайы (шәйнек)"},"price":"2 590","id":"2797bedc-f5d6-404c-9b0d-164230d7a056"},{"name":{"ru":"Чай облепиховый (чайник)","en":"Sea Buckthorn Tea (pot)","kk":"Шырғанақ шайы (шәйнек)"},"price":"2 690","id":"498e34a3-e6f3-4593-8e17-11b606f5c293"},{"name":{"ru":"Чай ягодный (чайник)","en":"Berry Tea (pot)","kk":"Жидек шайы (шәйнек)"},"price":"2 790","id":"71679e10-cb25-462e-b3bd-09bba08148ea"},{"name":{"ru":"Чай мараканский (чайник)","en":"Moroccan Tea (pot)","kk":"Мароккан шайы (шәйнек)"},"price":"2 990","id":"3f27a9a2-bc27-427f-91e2-ff3d1720d434"}],"id":"180213f2-95d7-461c-af69-38cca1ac4275"},{"title":{"ru":"Кофе","en":"Coffee","kk":"Кофе"},"items":[{"name":{"ru":"Jacobs (чашка)","en":"Jacobs (cup)","kk":"Jacobs (кесе)"},"price":"1 300","id":"9df61305-3bd7-44de-90f5-f11ad8ed7e50"}],"id":"b579ff6e-978c-46f1-8d30-4ded3875ba14"}],"kitchen":[{"title":{"ru":"Салаты","en":"Salads","kk":"Салаттар"},"items":[{"n":1,"name":{"ru":"Пекинский","en":"Peking Salad","kk":"Бейжің салаты"},"price":"3 690","desc":{"ru":"мясо, огурцы, болгарский перец, соус, лист салата","en":"meat, cucumber, bell pepper, sauce, lettuce","kk":"ет, қияр, бұрыш, тұздық, салат жапырағы"},"id":"1af21eed-3061-4a38-b545-fd5261e3e017"},{"n":2,"name":{"ru":"Греческий","en":"Greek Salad","kk":"Грек салаты"},"price":"3 590","desc":{"ru":"овощи, сыр фетакса, сливки","en":"vegetables, feta cheese, cream","kk":"көкөніс, фета ірімшігі, қаймақ"},"id":"644a9c19-3127-4ea5-8115-cfe401e6012c"},{"n":3,"name":{"ru":"Хрустящие баклажаны","en":"Crispy Eggplant","kk":"Қытырлақ баклажан"},"price":"3 890","desc":{"ru":"баклажан, помидор, соус","en":"eggplant, tomato, sauce","kk":"баклажан, қызанақ, тұздық"},"id":"d58ee10d-9494-4778-94b6-06320ef9a786"},{"n":4,"name":{"ru":"Теплый из требухи с овощами","en":"Warm Tripe with Vegetables","kk":"Көкөністі жылы қарын салаты"},"price":"3 890","id":"2c3140f4-64d5-4f82-b471-077d17851b11"},{"n":5,"name":{"ru":"Салат GO","en":"GO Salad","kk":"GO салаты"},"price":"3 890","desc":{"ru":"курица с апельсином, грибы, помидоры","en":"chicken with orange, mushrooms, tomatoes","kk":"тауық еті, апельсин, саңырауқұлақ, қызанақ"},"id":"ba786292-8f9b-4007-b3ee-54184e54eea9"}],"id":"d23c8bf0-76ba-45fc-ac13-d0db49d5a60e"},{"title":{"ru":"Холодные закуски","en":"Cold Appetizers","kk":"Салқын тағамдар"},"items":[{"n":1,"name":{"ru":"Ассорти под водочку","en":"Vodka Platter","kk":"Арақ ассорти"},"price":"3 990","desc":{"ru":"скумбрия, сельдь, долма, соленые огурчики","en":"mackerel, herring, dolma, pickles","kk":"скумбрия, майшабақ, толма, тұздалған қияр"},"id":"37fb50e2-47eb-4217-854a-7b4864552b01"},{"n":2,"name":{"ru":"Соленья","en":"Pickles","kk":"Тұздықтар"},"price":"3 790","desc":{"ru":"квашеная капуста, корнишоны, маринованные черри, оливки","en":"sauerkraut, gherkins, pickled cherry tomatoes, olives","kk":"ашытылған қырыққабат, қиярша, маринадталған қызанақ, зәйтүн"},"id":"a1d82c27-3abd-44d6-b4fd-db55bbdf8328"},{"n":3,"name":{"ru":"Кавказская нарезка","en":"Caucasian Platter","kk":"Кавказ асаны"},"price":"3 890","desc":{"ru":"огурцы, помидоры, болгарский перец, сыр Фета, зелень","en":"cucumber, tomato, bell pepper, feta cheese, herbs","kk":"қияр, қызанақ, бұрыш, фета ірімшігі, көк жиек"},"id":"215f0a6a-e359-48f9-8518-efc1f4f992f2"},{"n":4,"name":{"ru":"Мясная нарезка","en":"Meat Platter","kk":"Ет асаны"},"price":"6 590","desc":{"ru":"шужук, казы, мясо","en":"shuzhuk, kazy, meat","kk":"шұжық, қазы, ет"},"id":"fb246cad-7c4a-4923-aefd-c0a2a2080042"},{"n":5,"name":{"ru":"Холодец по-домашнему","en":"Homemade Aspic","kk":"Үй жасаған холодец"},"price":"3 650","id":"cea3c030-ef47-4925-a4e5-7ea0b4012980"},{"n":6,"name":{"ru":"Мужская закуска","en":"Hearty Platter","kk":"Ер-азамат тіскебасары"},"price":"6 590","desc":{"ru":"квашеная капуста, огурчики маринованные, курдючок баранины, холодец, горчица, черный хлеб","en":"sauerkraut, pickled cucumbers, lamb fat tail, aspic, mustard, black bread","kk":"ашытылған қырыққабат, маринадталған қияр, қой құйрығы, холодец, қыша, қара нан"},"id":"d449c176-1b30-47b5-b201-d5a5ae041f4c"},{"n":7,"name":{"ru":"Баклажаны в панировке","en":"Breaded Eggplant","kk":"Қамырланған баклажан"},"price":"3 790","id":"f137994f-d0d8-48ed-8549-3186927808cb"}],"id":"c8ba8167-a9f8-41cc-98ca-2a36ceabc492"},{"title":{"ru":"Горячие закуски","en":"Hot Appetizers","kk":"Ыстық тіскебасарлар"},"items":[{"n":1,"name":{"ru":"Крылья BBQ","en":"BBQ Wings","kk":"BBQ қанаттары"},"price":"3 590","id":"1ff7e573-2d64-4f57-98da-f71b4e2efc62"},{"n":2,"name":{"ru":"Сырные палочки","en":"Cheese Sticks","kk":"Ірімшік таяқшалары"},"price":"3 190","id":"ec24c8dc-f222-48a8-8036-ef666839075e"},{"n":3,"name":{"ru":"Нагетсы","en":"Nuggets","kk":"Наггетс"},"price":"2 890","id":"419e9c7a-f559-41d1-a03a-d891732b1219"},{"n":4,"name":{"ru":"Луковые кольца","en":"Onion Rings","kk":"Пияз сақиналары"},"price":"2 690","id":"e9f70644-d112-4b4e-a1a0-f77d6195f3f8"},{"n":5,"name":{"ru":"Гренки","en":"Croutons","kk":"Қытырлақ нан"},"price":"1 990","id":"b8397162-7cb3-461e-ab72-cdb3f0928650"},{"n":6,"name":{"ru":"Бараньи семечки","en":"Lamb Testicles","kk":"Қошқар жұмыртқасы"},"price":"3 950","id":"0fabb966-f2ca-4577-b999-69daa8813668"},{"n":7,"name":{"ru":"Жареные пельмени","en":"Fried Pelmeni","kk":"Қуырылған пельмень"},"price":"3 290","id":"f854a238-ad86-419f-b88f-d36c78a53a1f"},{"n":8,"name":{"ru":"Мини чебуречки","en":"Mini Chebureki","kk":"Мини шелпек"},"price":"2 690","id":"725628b8-2721-4aca-912d-5c02df58b04c"},{"n":9,"name":{"ru":"Печень с курдючком","en":"Liver with Fat Tail","kk":"Құйрықпен бауыр"},"price":"3 790","id":"369a7d89-e849-4531-9951-fe2115416258"}],"id":"29998c2c-a8d7-4bea-8bd5-eb3966b17bbd"},{"title":{"ru":"Супы","en":"Soups","kk":"Сорпалар"},"items":[{"n":1,"name":{"ru":"Пельмени домашние","en":"Homemade Pelmeni","kk":"Үй пельмені"},"price":"2 790","id":"57c38717-2894-4ba8-87f1-35f554957d79"},{"n":2,"name":{"ru":"Лапша домашняя","en":"Homemade Noodle Soup","kk":"Үй кеспесі"},"price":"2 290","id":"7eaae32b-cc6d-421f-b3d6-fcf51e3d91a5"},{"n":3,"name":{"ru":"Солянка","en":"Solyanka","kk":"Солянка"},"price":"2 690","id":"c7c0d8f6-702b-491c-8be7-e7f6d631c0ba"},{"n":4,"name":{"ru":"Том-Ям","en":"Tom Yum","kk":"Том-Ям"},"price":"3 590","id":"e76a8f7b-ab3e-46f9-892b-bf8d908f0edc"},{"n":5,"name":{"ru":"Рамён","en":"Ramen","kk":"Рамен"},"price":"2 990","id":"2e4bee77-8aed-4b12-96af-acfa304e69e8"}],"id":"67ec69cf-dcdd-4325-aa49-59ff53e05a45"},{"title":{"ru":"Море Go","en":"Sea GO","kk":"Теңіз GO"},"items":[{"n":1,"name":{"ru":"Креветки пивные","en":"Beer Shrimp","kk":"Сыралы асшаян"},"price":"4 290","id":"91b15800-ac05-4381-92d0-d4218890a1e1"},{"n":2,"name":{"ru":"Жаренные карасики с картофелем","en":"Fried Crucian Carp with Potatoes","kk":"Картоппен қуырылған табан балық"},"price":"3 590","id":"a3a8bcd7-c41f-4925-ac4d-918e8928ca07"},{"n":3,"name":{"ru":"Мойва","en":"Capelin","kk":"Мойва"},"price":"3 690","id":"2ae257a1-f473-4696-b291-ca55c43c33bf"},{"n":4,"name":{"ru":"Сазан на компанию с салатом по-домашнему","en":"Whole Carp to Share with Homemade Salad","kk":"Топқа арналған сазан, үй салаты"},"price":"16 590","id":"bbbbb318-7f2a-4096-b9ff-190cbe64a0e1"}],"id":"ec4aa39b-3667-458a-abda-39ae3e9bea85"},{"title":{"ru":"Пивной пир","en":"Beer Feast","kk":"Сыра мерекесі"},"note":{"ru":"сеты для компании, с пивом","en":"sharing sets with beer","kk":"топқа арналған, сырамен жинақтар"},"items":[{"n":1,"name":{"ru":"Рыбный сет + Пражское","en":"Fish Set + Prague Beer","kk":"Балық жинағы + Прага сырасы"},"price":"29 990","desc":{"ru":"сазан, мойва, караси, креветки, луковые кольца, картофельные дольки","en":"carp, capelin, crucian carp, shrimp, onion rings, potato wedges","kk":"сазан, мойва, табан балық, асшаян, пияз сақинасы, картоп бөлшектері"},"id":"0641fe27-7a8e-4d66-9289-a83d502d2c7a"},{"n":2,"name":{"ru":"Куриный сет + Пражское","en":"Chicken Set + Prague Beer","kk":"Тауық жинағы + Прага сырасы"},"price":"24 990","desc":{"ru":"BBQ, наггетсы, kfc, луковые кольца, сырные палочки, фри","en":"BBQ wings, nuggets, kfc-style chicken, onion rings, cheese sticks, fries","kk":"BBQ қанат, наггетс, kfc тауық, пияз сақинасы, ірімшік таяқша, фри"},"id":"0aa055dd-2b8b-46e4-a976-78191b5ecf21"},{"n":3,"name":{"ru":"Мясная доска + Баварское нефильтрованное","en":"Meat Board + Bavarian Unfiltered","kk":"Ет тақтасы + Бавария сырасы"},"price":"31 990","desc":{"ru":"рибай, тибон, стейк куриный, утка, колбасы, цыпленок табака, овощи гриль","en":"ribeye, t-bone, chicken steak, duck, sausages, chicken tabaka, grilled vegetables","kk":"рибай, ти-бон, тауық стейгі, үйрек, шұжық, тәбака тауығы, грильдегі көкөніс"},"id":"fa8877c6-0a1e-4a5d-a5bf-464289e06da3"},{"n":4,"name":{"ru":"Улов рыбака + Баварское нефильтрованное","en":"Fisherman's Catch + Bavarian Unfiltered","kk":"Балықшы уловы + Бавария сырасы"},"price":"21 990","desc":{"ru":"копченая, вяленая, сушеная рыбка","en":"smoked, cured, dried fish","kk":"ыстатылған, кептірілген балық"},"id":"38356b34-7454-4e3c-baeb-8726554a7324"},{"n":5,"name":{"ru":"Шашлычный сет GO + Пражское или Баварское нефильтрованное","en":"GO Skewer Set + Prague or Bavarian Unfiltered","kk":"GO шашлық жинағы + Прага/Бавария сырасы"},"price":"35 990","desc":{"ru":"2 баранины, 2 утки, 2 филе, 2 крыла, грибы, овощи, картофельные дольки","en":"2 lamb, 2 duck, 2 fillet, 2 wings, mushrooms, vegetables, potato wedges","kk":"2 қой еті, 2 үйрек, 2 филе, 2 қанат, саңырауқұлақ, көкөніс, картоп бөлшектері"},"id":"49e1d8a0-acf9-44ba-ba40-b5bb62e16f46"}],"id":"a94d59c0-05b7-41a2-9903-f738beae7e74"},{"title":{"ru":"Street food","en":"Street Food","kk":"Көше тағамдары"},"items":[{"n":1,"name":{"ru":"Beef Burger","en":"Beef Burger","kk":"Бифбургер"},"price":"3 890","id":"5c6ed7e7-4164-4bbe-9d29-ca3303697e73"},{"n":2,"name":{"ru":"Chicken Burger","en":"Chicken Burger","kk":"Тауықбургер"},"price":"3 690","id":"cfdd4dec-2f8e-447b-9520-c5b9137df4d2"},{"n":3,"name":{"ru":"Шаурма с курицей","en":"Chicken Shawarma","kk":"Тауық шаурмасы"},"price":"3 590","id":"6fbe2d66-3898-4db1-bd34-c9dcbcfd169f"},{"n":4,"name":{"ru":"Куриный пирог GO","en":"GO Chicken Pie","kk":"GO тауық пирогы"},"price":"5 390","id":"480e06a2-34e7-4a3a-9ffc-c91f78e04c68"},{"n":5,"name":{"ru":"Баскет с фри","en":"Basket with Fries","kk":"Фримен себет"},"price":"9 290","id":"39e075d0-6303-46a8-b66c-c19065416f80"}],"id":"3b9249f0-97c3-44cb-baea-bbbcc5a190bc"},{"title":{"ru":"Пицца","en":"Pizza","kk":"Пицца"},"items":[{"n":1,"name":{"ru":"Курица с грибами","en":"Chicken & Mushroom","kk":"Тауық пен саңырауқұлақ"},"price":"3 890","desc":{"ru":"моцарелла, филе курицы, соус, шампиньоны","en":"mozzarella, chicken fillet, sauce, mushrooms","kk":"моцарелла, тауық филесі, тұздық, шампиньон"},"id":"519ab09c-92d1-40b8-8b3b-9f5d97987231"},{"n":2,"name":{"ru":"Мексикано","en":"Mexicano","kk":"Мексикано"},"price":"3 990","desc":{"ru":"моцарелла, мясо говядины, болгарский перец, халапеньо","en":"mozzarella, beef, bell pepper, jalapeño","kk":"моцарелла, сиыр еті, бұрыш, халапеньо"},"id":"61d7bbcb-d8f0-4c03-81b5-ec217e291a1d"},{"n":3,"name":{"ru":"Пицца 4 сезона","en":"Four Seasons","kk":"Төрт маусым"},"price":"3 890","desc":{"ru":"моцарелла, салями, грибы, мясо говядины, помидоры","en":"mozzarella, salami, mushrooms, beef, tomatoes","kk":"моцарелла, салями, саңырауқұлақ, сиыр еті, қызанақ"},"id":"7005c72f-b3fd-4b8e-9a58-fce5f60c29c1"},{"n":4,"name":{"ru":"Маргарита","en":"Margherita","kk":"Маргарита"},"price":"3 190","desc":{"ru":"моцарелла, помидоры, соус","en":"mozzarella, tomatoes, sauce","kk":"моцарелла, қызанақ, тұздық"},"id":"6c2ef96f-20a9-4396-a7cd-746f6fc58385"},{"n":5,"name":{"ru":"Пепперони","en":"Pepperoni","kk":"Пепперони"},"price":"3 890","desc":{"ru":"моцарелла, копченая колбаса","en":"mozzarella, smoked sausage","kk":"моцарелла, ысталған шұжық"},"id":"bcf2c22b-0b78-426b-bf7e-cf975e853782"},{"n":6,"name":{"ru":"Сырная","en":"Four Cheese","kk":"Ірімшікті"},"price":"3 990","id":"def5e6eb-11b2-4147-a2f8-e0d575ea693d"}],"id":"c4dee86b-eee9-4dc2-9a84-adb369e05200"},{"title":{"ru":"Паста","en":"Pasta","kk":"Паста"},"items":[{"n":1,"name":{"ru":"Альфредо с фетучини","en":"Fettuccine Alfredo","kk":"Феттучини Альфредо"},"price":"3 790","id":"0f662730-685e-4559-a007-456ed63155e8"},{"n":2,"name":{"ru":"Болоньезе","en":"Bolognese","kk":"Болоньезе"},"price":"3 790","id":"39eebd3e-dac7-49af-a4a9-74d83122a841"}],"id":"f3727e50-6725-4106-aa33-962b5d8307f0"},{"title":{"ru":"Шашлык","en":"Skewers","kk":"Шашлық"},"items":[{"n":1,"name":{"ru":"Баранина","en":"Lamb","kk":"Қой еті"},"price":"5 000","id":"e15313f2-dcf4-4e57-9a37-fd1c5a5d6083"},{"n":2,"name":{"ru":"Телятина","en":"Veal","kk":"Бұзау еті"},"price":"5 000","id":"4ceb1497-8147-4923-862b-24c2edbc2301"},{"n":3,"name":{"ru":"Утка","en":"Duck","kk":"Үйрек"},"price":"4 590","id":"b86811d9-37b8-43bb-9f6c-fd5b58122eba"},{"n":4,"name":{"ru":"Крылышки","en":"Wings","kk":"Қанат"},"price":"3 990","id":"7fcd78f7-3a47-4140-8a5f-1bfb4f7ff143"},{"n":5,"name":{"ru":"Куриное филе","en":"Chicken Fillet","kk":"Тауық филесі"},"price":"3 750","id":"c76c96dc-90cb-47ff-8f3b-2f42ccf13838"},{"n":6,"name":{"ru":"Грибы","en":"Mushrooms","kk":"Саңырауқұлақ"},"price":"3 750","id":"6483e993-3bc6-4d21-abad-816d9274a927"},{"n":7,"name":{"ru":"Картофельный","en":"Potato","kk":"Картоп"},"price":"2 450","id":"0088891d-054a-468c-8716-568c52658946"},{"n":8,"name":{"ru":"Овощи гриль","en":"Grilled Vegetables","kk":"Грильдегі көкөніс"},"price":"2 890","id":"cccd924f-246c-4a4e-b9a4-abfc9c4e80a5"}],"id":"931d180c-1af4-44b1-a3ab-cb0258f86e55"},{"title":{"ru":"Горячие блюда","en":"Main Courses","kk":"Ыстық тағамдар"},"items":[{"n":1,"name":{"ru":"Стейк Рибай","en":"Ribeye Steak","kk":"Рибай стейгі"},"price":"7 390","id":"54682a93-8624-413b-a782-17e0bf70fb47"},{"n":2,"name":{"ru":"Стейк Ти-бон","en":"T-Bone Steak","kk":"Ти-бон стейгі"},"price":"7 390","id":"341ec0a4-e2d6-4af9-8d91-05b8f47391e3"},{"n":3,"name":{"ru":"Микс на жаровне","en":"Grill Mix","kk":"Грильдегі ассорти"},"price":"4 490","id":"a872e8a0-e57c-4b7e-a751-9b74c575641c"},{"n":4,"name":{"ru":"Мясо по-мексикански","en":"Mexican-Style Meat","kk":"Мексикалық ет"},"price":"4 890","id":"89025a3d-62b1-48d6-b173-8f5375321eec"},{"n":5,"name":{"ru":"Медальоны в сливочно-грибном соусе","en":"Medallions in Creamy Mushroom Sauce","kk":"Қаймақ-саңырауқұлақ тұздығындағы медальон"},"price":"5 290","id":"1d430d2c-d563-4308-bef9-c8edffce4815"},{"n":6,"name":{"ru":"Куырдак","en":"Kuyrdak","kk":"Куырдақ"},"price":"5 290","id":"7ecbce9f-30f2-4997-918c-4cfead69a9bf"},{"n":7,"name":{"ru":"Курица с грибами в сливочном соусе с пюре","en":"Chicken & Mushrooms in Cream Sauce with Mash","kk":"Пюремен қаймақты тұздықтағы тауық"},"price":"4 190","id":"cf8d39c2-8ead-4a41-bd4d-aab944aac1ef"},{"n":8,"name":{"ru":"Говядина с шампиньонами и рисом","en":"Beef with Mushrooms and Rice","kk":"Күрішпен, шампиньонмен сиыр еті"},"price":"4 790","id":"80cae8fd-54b1-427f-802b-8fd349426fd4"}],"id":"aa99406b-c9c9-4a63-b654-c43de30474e9"},{"title":{"ru":"Сеты на компанию","en":"Sharing Sets","kk":"Топқа арналған жинақтар"},"items":[{"n":1,"name":{"ru":"Пивная доска №1","en":"Beer Board #1","kk":"Сыра тақтасы №1"},"price":"4 990","desc":{"ru":"чебуреки, гренки, редис, соусы","en":"chebureki, croutons, radish, sauces","kk":"шелпек, қытырлақ нан, шалғам, тұздықтар"},"id":"c02a4db8-3f2d-4964-934c-994e50d69869"},{"n":2,"name":{"ru":"Пивная доска №2","en":"Beer Board #2","kk":"Сыра тақтасы №2"},"price":"11 590","desc":{"ru":"пельмени жареные, сырные палочки, луковые кольца, гренки, соусы","en":"fried pelmeni, cheese sticks, onion rings, croutons, sauces","kk":"қуырылған пельмень, ірімшік таяқша, пияз сақинасы, қытырлақ нан, тұздықтар"},"id":"a4aec786-a2e0-4e9f-a01b-9a0f2ffad435"},{"n":3,"name":{"ru":"Пивная доска №3","en":"Beer Board #3","kk":"Сыра тақтасы №3"},"price":"14 490","desc":{"ru":"крылья BBQ, бараньи семечки, сырные палочки, наггетсы, соусы, орешки","en":"BBQ wings, lamb testicles, cheese sticks, nuggets, sauces, nuts","kk":"BBQ қанат, қошқар жұмыртқасы, ірімшік таяқша, наггетс, тұздықтар, жаңғақ"},"id":"02dac13c-1768-4d0d-8149-631a88a558dd"},{"n":4,"name":{"ru":"Ассорти колбасок, дольки, соусы","en":"Sausage Assortment, Wedges, Sauces","kk":"Шұжық ассортиі, картоп бөлшектері, тұздықтар"},"price":"12 690","id":"5d6969a0-a85d-4d21-8616-972cc3488fc8"},{"n":5,"name":{"ru":"Пивной микс","en":"Beer Mix","kk":"Сыра миксі"},"price":"7 690","desc":{"ru":"чечил, фисташки, орешки, чипсы, сухарики, курт","en":"chechil cheese, pistachios, nuts, chips, croutons, kurt","kk":"шешіл ірімшігі, фисташка, жаңғақ, чипсы, кептірілген нан, құрт"},"id":"85c13bac-de50-4bf9-9719-8b8ce18e4194"},{"n":6,"name":{"ru":"Ведро креветок","en":"Bucket of Shrimp","kk":"Асшаян шелегі"},"price":"14 990","id":"85d32b8a-561c-4164-b0f9-c23449a5df3b"},{"n":7,"name":{"ru":"Стейки 2+1","en":"Steaks 2+1","kk":"Стейктер 2+1"},"price":"16 990","id":"a35f15fa-2ca9-40cc-b746-bb0792fbed52"}],"id":"7ea252be-dc80-4a61-8c74-182211fce64e"},{"title":{"ru":"Гарнир","en":"Sides","kk":"Гарнирлер"},"items":[{"n":1,"name":{"ru":"Рис","en":"Rice","kk":"Күріш"},"price":"1 450","id":"dd96294b-116e-447b-a230-7e83996fb09e"},{"n":2,"name":{"ru":"Картофельный фри","en":"French Fries","kk":"Картоп фри"},"price":"1 500","id":"4caa6b23-690a-4c75-a45c-14912c818be7"},{"n":3,"name":{"ru":"Картофельные дольки","en":"Potato Wedges","kk":"Картоп бөлшектері"},"price":"1 550","id":"7367008b-bbdb-4701-9655-e02a9fb31a3c"},{"n":4,"name":{"ru":"Картошка по-домашнему","en":"Home-style Potatoes","kk":"Үй картобы"},"price":"2 790","id":"1851f45c-c488-45c4-a5f0-976af2464021"},{"n":5,"name":{"ru":"Соусы","en":"Sauces","kk":"Тұздықтар"},"price":"800","id":"482d7648-d65e-48d8-86b6-dddc8319cb9a"}],"id":"4434aa40-f629-4976-840c-c1d9b0cc6f55"},{"title":{"ru":"Дессерт","en":"Dessert","kk":"Десерттер"},"items":[{"n":1,"name":{"ru":"Фруктовая нарезка","en":"Fruit Platter","kk":"Жеміс тілімдері"},"price":"7 990","id":"450d093d-afb0-4a14-8cee-74ab3c87be77"},{"n":2,"name":{"ru":"Сладкий десерт","en":"Sweet Dessert","kk":"Тәтті десерт"},"price":"2 190","id":"d4272368-6a15-4466-8836-1e44e7ddcdec"},{"n":3,"name":{"ru":"Мороженое","en":"Ice Cream","kk":"Балмұздақ"},"price":"1 990","id":"92318b39-b88b-448e-b53b-ad88016e4797"},{"n":4,"name":{"ru":"Лимон","en":"Lemon","kk":"Лимон"},"price":"890","id":"bc38e363-4079-4281-97fe-58dc0d855b3d"},{"n":5,"name":{"ru":"Хлебная корзина","en":"Bread Basket","kk":"Нан себеті"},"price":"800","id":"9459e27c-7a0a-4ecd-9f3e-e7abeb75eaa0"},{"n":6,"name":{"ru":"Хлебная корзина 1/2","en":"Bread Basket (half)","kk":"Нан себеті 1/2"},"price":"400","id":"ec46aac7-ef2a-42a0-be25-2e19c7769932"}],"id":"85caf3f1-9e31-43c7-b36b-cb55f506fdd9"}]};

// ---------------------------------------------------------------------
// Web Push (RFC 8291 payload encryption + RFC 8292 VAPID), implemented
// with the platform's built-in Web Crypto API only — no npm dependency,
// since this Worker is deployed as a single plain file.
// ---------------------------------------------------------------------

function b64urlToBytes(s) {
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
function bytesToB64url(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function concatBytes(...arrs) {
  const len = arrs.reduce((s, a) => s + a.length, 0);
  const out = new Uint8Array(len);
  let off = 0;
  for (const a of arrs) { out.set(a, off); off += a.length; }
  return out;
}
async function hmacSha256(keyBytes, dataBytes) {
  const key = await crypto.subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", key, dataBytes));
}
async function hkdfExpand(prk, info, length) {
  const t1 = await hmacSha256(prk, concatBytes(info, new Uint8Array([1])));
  return t1.slice(0, length);
}

async function encryptWebPushPayload(payloadBytes, p256dhB64url, authB64url) {
  const clientPublicKeyBytes = b64urlToBytes(p256dhB64url); // 65 bytes
  const authSecret = b64urlToBytes(authB64url); // 16 bytes

  const clientKey = await crypto.subtle.importKey("raw", clientPublicKeyBytes, { name: "ECDH", namedCurve: "P-256" }, false, []);
  const serverKeyPair = await crypto.subtle.generateKey({ name: "ECDH", namedCurve: "P-256" }, true, ["deriveBits"]);
  const serverPublicKeyBytes = new Uint8Array(await crypto.subtle.exportKey("raw", serverKeyPair.publicKey));

  const sharedSecret = new Uint8Array(await crypto.subtle.deriveBits({ name: "ECDH", public: clientKey }, serverKeyPair.privateKey, 256));

  const prkKey = await hmacSha256(authSecret, sharedSecret);
  const keyInfo = concatBytes(
    new TextEncoder().encode("WebPush: info"),
    new Uint8Array([0]),
    clientPublicKeyBytes,
    serverPublicKeyBytes
  );
  const ikm = await hkdfExpand(prkKey, keyInfo, 32);

  const salt = crypto.getRandomValues(new Uint8Array(16));
  const prk = await hmacSha256(salt, ikm);

  const cek = await hkdfExpand(prk, concatBytes(new TextEncoder().encode("Content-Encoding: aes128gcm"), new Uint8Array([0])), 16);
  const nonce = await hkdfExpand(prk, concatBytes(new TextEncoder().encode("Content-Encoding: nonce"), new Uint8Array([0])), 12);

  const paddedPlaintext = concatBytes(payloadBytes, new Uint8Array([2])); // delimiter for a single (final) record
  const cekKey = await crypto.subtle.importKey("raw", cek, { name: "AES-GCM" }, false, ["encrypt"]);
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce, tagLength: 128 }, cekKey, paddedPlaintext));

  const rsBytes = new Uint8Array(4);
  new DataView(rsBytes.buffer).setUint32(0, 4096, false);
  const header = concatBytes(salt, rsBytes, new Uint8Array([serverPublicKeyBytes.length]), serverPublicKeyBytes);

  return concatBytes(header, ciphertext);
}

async function buildVapidAuthHeader(endpoint, subject, publicKeyB64url, privateKeyPkcs8B64) {
  const aud = new URL(endpoint).origin;
  const header = { typ: "JWT", alg: "ES256" };
  const claims = { aud, exp: Math.floor(Date.now() / 1000) + 12 * 3600, sub: subject };
  const enc = (obj) => bytesToB64url(new TextEncoder().encode(JSON.stringify(obj)));
  const signingInput = enc(header) + "." + enc(claims);

  const pkcs8Bytes = Uint8Array.from(atob(privateKeyPkcs8B64), c => c.charCodeAt(0));
  const privateKey = await crypto.subtle.importKey("pkcs8", pkcs8Bytes, { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, privateKey, new TextEncoder().encode(signingInput)));

  return `vapid t=${signingInput}.${bytesToB64url(sig)}, k=${publicKeyB64url}`;
}

async function sendWebPush(subscription, payloadObj, env) {
  const payloadBytes = new TextEncoder().encode(JSON.stringify(payloadObj));
  const body = await encryptWebPushPayload(payloadBytes, subscription.keys.p256dh, subscription.keys.auth);
  const auth = await buildVapidAuthHeader(
    subscription.endpoint,
    env.VAPID_SUBJECT || "mailto:admin@example.com",
    env.VAPID_PUBLIC_KEY,
    env.VAPID_PRIVATE_KEY_PKCS8
  );
  return fetch(subscription.endpoint, {
    method: "POST",
    headers: {
      Authorization: auth,
      "Content-Type": "application/octet-stream",
      "Content-Encoding": "aes128gcm",
      TTL: "60",
    },
    body,
  });
}

export class OrderBoard {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = new Set(); // { ws, role, authed }
    this.orders = [];
    this.history = [];
    this.openTables = {};
    this.closedTables = [];
    this.staff = [];       // [{id, role, name}]
    this.rooms = [{ id: "default", name: "Зал", tableCount: 20 }]; // [{id, name, tableCount}]
    this.unavailable = {}; // key "kitchen|Name" or "bar|Name" -> {comment, disabledBy, disabledAt}
    this.pins = { waiter: "1111", cook: "1111", bartender: "1111", manager: "1111" };
    this.inventory = { kitchen: {}, bar: {} }; // dest -> { "Name": {qty, threshold} }
    this.pushSubs = []; // [{id, role, staffId, subscription}]
    this.actionLog = []; // [{ts, role, staffName, action, details}]
    this.menu = DEFAULT_MENU; // { kitchen: [...], bar: [...] } — categories/items carry stable ids
    this.ready = this.state.blockConcurrencyWhile(async () => {
      const storedOrders = await this.state.storage.get("orders");
      const storedHistory = await this.state.storage.get("history");
      const storedOpen = await this.state.storage.get("openTables");
      const storedClosed = await this.state.storage.get("closedTables");
      const storedStaff = await this.state.storage.get("staff");
      const storedRooms = await this.state.storage.get("rooms");
      const storedTableCount = await this.state.storage.get("tableCount"); // legacy, pre-rooms
      const storedUnavailable = await this.state.storage.get("unavailable");
      const storedPins = await this.state.storage.get("pins");
      const storedInventory = await this.state.storage.get("inventory");
      const storedPushSubs = await this.state.storage.get("pushSubs");
      const storedActionLog = await this.state.storage.get("actionLog");
      const storedMenu = await this.state.storage.get("menu");
      if (storedOrders) this.orders = storedOrders;
      if (storedHistory) this.history = storedHistory;
      if (storedOpen) this.openTables = storedOpen;
      if (storedClosed) this.closedTables = storedClosed;
      if (storedStaff) this.staff = storedStaff;
      if (storedRooms) this.rooms = storedRooms;
      else if (storedTableCount) this.rooms = [{ id: "default", name: "Зал", tableCount: storedTableCount }];
      if (storedUnavailable) this.unavailable = storedUnavailable;
      if (storedPins) this.pins = storedPins;
      if (storedInventory) this.inventory = storedInventory;
      if (storedPushSubs) this.pushSubs = storedPushSubs;
      if (storedActionLog) this.actionLog = storedActionLog;
      if (storedMenu) this.menu = storedMenu;
    });
  }

  async persist() {
    await this.state.storage.put("orders", this.orders);
    await this.state.storage.put("history", this.history);
    await this.state.storage.put("openTables", this.openTables);
    await this.state.storage.put("closedTables", this.closedTables);
    await this.state.storage.put("staff", this.staff);
    await this.state.storage.put("rooms", this.rooms);
    await this.state.storage.put("unavailable", this.unavailable);
    await this.state.storage.put("pins", this.pins);
    await this.state.storage.put("inventory", this.inventory);
    await this.state.storage.put("pushSubs", this.pushSubs);
    await this.state.storage.put("actionLog", this.actionLog);
    await this.state.storage.put("menu", this.menu);
  }

  log(conn, action, details) {
    this.actionLog.push({
      ts: Date.now(),
      role: conn.role || null,
      staffName: conn.staffName || null,
      action,
      details: details || {},
    });
    if (this.actionLog.length > 1000) this.actionLog = this.actionLog.slice(-1000);
  }

  stateSnapshot() {
    return {
      type: "state",
      orders: this.orders,
      history: this.history,
      openTables: this.openTables,
      closedTables: this.closedTables,
      staff: this.staff,
      rooms: this.rooms,
      tableCount: this.rooms.reduce((s, r) => s + (r.tableCount || 0), 0), // kept for any old client still reading it
      unavailable: this.unavailable,
      inventory: this.inventory,
      actionLog: this.actionLog,
      menu: this.menu,
      vapidPublicKey: this.env.VAPID_PUBLIC_KEY || null,
    };
  }

  // What a guest (the public customer menu, no login) is allowed to see —
  // just the menu and availability, never other tables' orders, staff
  // names, or revenue.
  guestSnapshot() {
    return {
      type: "state",
      menu: this.menu,
      unavailable: this.unavailable,
      vapidPublicKey: this.env.VAPID_PUBLIC_KEY || null,
    };
  }

  computeTableBill(table) {
    const session = this.openTables[table];
    if (!session) return { hasOrders: false, items: [], subtotal: 0, service: 0, total: 0 };
    const rounds = this.history.filter(h => h.table === table && h.servedAt >= session.openedAt);
    const activeOrders = this.orders.filter(o => o.table === table);
    const merged = {};
    const addItems = (items) => {
      (items || []).forEach(i => {
        const key = i.name;
        if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price };
        merged[key].qty += i.qty;
      });
    };
    rounds.forEach(r => { addItems(r.kitchenItems); addItems(r.barItems); });
    activeOrders.forEach(o => { addItems(o.kitchenItems); addItems(o.barItems); });
    const items = Object.values(merged);
    const subtotal = items.reduce((s, i) => s + parsePrice(i.price) * i.qty, 0);
    const service = Math.round(subtotal * SERVICE_RATE);
    return { hasOrders: items.length > 0, items, subtotal, service, total: subtotal + service };
  }

  broadcast() {
    const fullPayload = JSON.stringify(this.stateSnapshot());
    const guestPayload = JSON.stringify(this.guestSnapshot());
    for (const client of this.sockets) {
      try { client.ws.send(client.role === "guest" ? guestPayload : fullPayload); } catch (e) { /* ignore dead sockets */ }
    }
  }

  notify(role, message) {
    const payload = JSON.stringify({ type: "notify", ...message });
    for (const client of this.sockets) {
      if (client.role === role) {
        try { client.ws.send(payload); } catch (e) {}
      }
    }
    this.pushToRole(role, message);
  }

  pushText(message) {
    if (message.kind === "low_stock") return { title: "GO pub — заканчивается", body: `${message.name} — осталось ${message.qty}` };
    if (message.kind === "call_waiter") return { title: "GO pub — зовут официанта", body: `Стол ${message.table}` };
    if (message.kind === "request_bill") return { title: "GO pub — просят счёт", body: `Стол ${message.table}` };
    if (message.part) return { title: "GO pub — готово", body: `Стол ${message.table} (${message.part === "kitchen" ? "кухня" : "бар"})` };
    if (message.table !== undefined) return { title: "GO pub — новый заказ", body: `Стол ${message.table}` };
    return { title: "GO pub", body: "Новое уведомление" };
  }

  pushToRole(role, message) {
    const subs = this.pushSubs.filter(p => p.role === role);
    if (subs.length === 0) return;
    const text = this.pushText(message);
    subs.forEach(p => {
      sendWebPush(p.subscription, text, this.env)
        .then(async (resp) => {
          if (resp && (resp.status === 404 || resp.status === 410)) {
            this.pushSubs = this.pushSubs.filter(x => x.id !== p.id);
            await this.persist();
          }
        })
        .catch(() => {});
    });
  }

  sendTo(conn, message) {
    try { conn.ws.send(JSON.stringify(message)); } catch (e) {}
  }

  consumeStock(dest, items, notifyRole) {
    (items || []).forEach(i => {
      const stock = this.inventory[dest][i.name];
      if (!stock) return; // not tracked — nothing to do
      stock.qty = Math.max(0, stock.qty - (i.qty || 1));
      if (stock.qty <= stock.threshold) {
        this.notify(notifyRole, { kind: "low_stock", name: i.name, qty: stock.qty });
      }
    });
  }

  checkPin(role, pin) {
    const expected = this.pins[role];
    if (!expected) return true; // no PIN configured for this role -> allow
    return String(pin || "") === String(expected);
  }

  async fetch(request) {
    await this.ready;
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      const conn = { ws: server, role: null, authed: false };
      this.sockets.add(conn);

      server.addEventListener("message", async (evt) => {
        let msg;
        try { msg = JSON.parse(evt.data); } catch (e) { return; }

        if (msg.type === "hello") {
          if (msg.role === "guest") {
            // Public, read-only access for the customer-facing menu page —
            // no PIN or staff link needed, and it can never reach any of
            // the role-gated write branches below.
            conn.role = "guest";
            conn.authed = true;
          } else if (msg.staffId) {
            const staff = this.staff.find(s => s.id === msg.staffId && s.role === msg.role);
            if (!staff) {
              server.send(JSON.stringify({ type: "auth_error", reason: "unknown_staff" }));
              server.close(4001, "unknown staff");
              this.sockets.delete(conn);
              return;
            }
            conn.role = msg.role;
            conn.staffId = staff.id;
            conn.staffName = staff.name || "";
            conn.authed = true;
          } else if (msg.role !== "manager") {
            // Waiter/cook/bartender can no longer fall back to a shared PIN —
            // a valid personal QR link (staffId) is required. This is what
            // makes deleting/reassigning a staff member actually revoke access.
            server.send(JSON.stringify({ type: "auth_error", reason: "staff_required" }));
            server.close(4001, "staff id required");
            this.sockets.delete(conn);
            return;
          } else {
            if (!this.checkPin(msg.role, msg.pin)) {
              server.send(JSON.stringify({ type: "auth_error" }));
              server.close(4001, "bad pin");
              this.sockets.delete(conn);
              return;
            }
            conn.role = msg.role;
            conn.authed = true;
          }
          server.send(JSON.stringify({ type: "auth_ok" }));
          server.send(JSON.stringify(conn.role === "guest" ? this.guestSnapshot() : this.stateSnapshot()));
          return;
        }

        if (msg.type === "get_receipt") {
          // Public, unauthenticated lookup — this is what a QR code or a
          // shared WhatsApp/Telegram link points a guest or a manager to.
          const receipt = this.closedTables.find(r => r.id === msg.id);
          this.sendTo(conn, receipt
            ? { type: "receipt_data", receipt }
            : { type: "receipt_not_found", id: msg.id });
          return;
        }

        if (!conn.authed) return; // ignore everything until hello succeeds

        if (msg.type === "get_table_bill" && msg.table) {
          this.sendTo(conn, { type: "table_bill", table: msg.table, ...this.computeTableBill(msg.table) });
        }

        if (msg.type === "call_waiter" && msg.table) {
          this.notify("waiter", { kind: "call_waiter", table: msg.table });
          this.log(conn, "call_waiter", { table: msg.table });
        }

        if (msg.type === "request_bill" && msg.table) {
          this.notify("waiter", { kind: "request_bill", table: msg.table });
          this.log(conn, "request_bill", { table: msg.table });
        }

        if (msg.type === "register_push" && msg.pushId && msg.subscription) {
          this.pushSubs = this.pushSubs.filter(p => p.id !== msg.pushId);
          this.pushSubs.push({ id: msg.pushId, role: conn.role, staffId: conn.staffId || null, subscription: msg.subscription });
          await this.persist();
        }

        if (msg.type === "unregister_push" && msg.pushId) {
          this.pushSubs = this.pushSubs.filter(p => p.id !== msg.pushId);
          await this.persist();
        }

        if (msg.type === "set_staff_name") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff && conn.staffId === msg.staffId) {
            staff.name = String(msg.name || "").slice(0, 40);
            conn.staffName = staff.name;
            this.log(conn, "set_name", { name: staff.name });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "set_display_name" && !conn.staffId) {
          // For connections logged in with the shared role PIN (no personal
          // staff record) — the name only lives on this connection and is
          // re-sent by the client after every reconnect.
          conn.staffName = String(msg.name || "").slice(0, 40);
        }

        if (msg.type === "change_pin" && conn.role === msg.role) {
          const newPin = String(msg.newPin || "").trim();
          if (newPin.length >= 4 && newPin.length <= 6 && /^\d+$/.test(newPin)) {
            this.pins[msg.role] = newPin;
            this.log(conn, "change_pin", { role: msg.role });
            await this.persist();
            this.sendTo(conn, { type: "pin_changed", role: msg.role });
          } else {
            this.sendTo(conn, { type: "pin_change_error", reason: "invalid" });
          }
        }

        if (msg.type === "create_staff" && conn.role === "manager") {
          const staff = { id: crypto.randomUUID(), role: msg.role, name: "" };
          this.staff.push(staff);
          this.log(conn, "create_staff", { role: msg.role });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "delete_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          this.staff = this.staff.filter(s => s.id !== msg.staffId);
          this.log(conn, "delete_staff", { role: staff ? staff.role : null, name: staff ? staff.name : null });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "reassign_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff) {
            staff.id = crypto.randomUUID(); // old device's cached id stops matching anyone
            this.log(conn, "reassign_staff", { role: staff.role, name: staff.name });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "create_room" && conn.role === "manager") {
          const room = { id: crypto.randomUUID(), name: String(msg.name || "Зал").slice(0, 40), tableCount: 10 };
          this.rooms.push(room);
          this.log(conn, "create_room", { name: room.name });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "update_room" && conn.role === "manager") {
          const room = this.rooms.find(r => r.id === msg.roomId);
          if (room) {
            if (msg.name !== undefined) room.name = String(msg.name).slice(0, 40) || room.name;
            if (msg.tableCount !== undefined) {
              const n = parseInt(msg.tableCount, 10);
              if (n && n > 0 && n <= 200) room.tableCount = n;
            }
            this.log(conn, "update_room", { name: room.name, tableCount: room.tableCount });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "delete_room" && conn.role === "manager") {
          if (this.rooms.length > 1) {
            const room = this.rooms.find(r => r.id === msg.roomId);
            this.rooms = this.rooms.filter(r => r.id !== msg.roomId);
            this.log(conn, "delete_room", { name: room ? room.name : null });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "add_menu_category" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = { id: crypto.randomUUID(), title: { ru: String(msg.title || "Раздел").slice(0, 40) }, items: [] };
          this.menu[dest].push(cat);
          this.log(conn, "add_menu_category", { dest, title: cat.title.ru });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "rename_menu_category" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          if (cat) {
            cat.title.ru = String(msg.title || cat.title.ru).slice(0, 40);
            this.log(conn, "rename_menu_category", { dest, title: cat.title.ru });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "delete_menu_category" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          this.menu[dest] = this.menu[dest].filter(c => c.id !== msg.categoryId);
          this.log(conn, "delete_menu_category", { dest, title: cat ? cat.title.ru : null });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "add_menu_item" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          if (cat) {
            const item = {
              id: crypto.randomUUID(),
              name: { ru: String(msg.name || "Новая позиция").slice(0, 60) },
              price: String(msg.price || "0").slice(0, 20),
            };
            if (msg.desc) item.desc = { ru: String(msg.desc).slice(0, 200) };
            cat.items.push(item);
            this.log(conn, "add_menu_item", { dest, name: item.name.ru });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "update_menu_item" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          const item = cat && cat.items.find(i => i.id === msg.itemId);
          if (item) {
            if (msg.name !== undefined) item.name.ru = String(msg.name).slice(0, 60) || item.name.ru;
            if (msg.price !== undefined) item.price = String(msg.price).slice(0, 20) || item.price;
            if (msg.desc !== undefined) {
              if (msg.desc) { item.desc = item.desc || {}; item.desc.ru = String(msg.desc).slice(0, 200); }
              else delete item.desc;
            }
            this.log(conn, "update_menu_item", { dest, name: item.name.ru, price: item.price });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "delete_menu_item" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          if (cat) {
            const item = cat.items.find(i => i.id === msg.itemId);
            cat.items = cat.items.filter(i => i.id !== msg.itemId);
            this.log(conn, "delete_menu_item", { dest, name: item ? item.name.ru : null });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "disable_items" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          (msg.items || []).forEach(i => {
            this.unavailable[dest + "|" + i.name] = {
              comment: String(i.comment || "").slice(0, 200),
              disabledBy: conn.staffName || null,
              disabledAt: Date.now(),
            };
          });
          this.log(conn, "disable_items", { dest, names: (msg.items || []).map(i => i.name) });
          await this.persist();
          this.broadcast();
          this.notify("manager", { kind: "menu_disabled", dest, names: (msg.items || []).map(i => i.name) });
        }

        if (msg.type === "enable_items" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          (msg.names || []).forEach(name => { delete this.unavailable[dest + "|" + name]; });
          this.log(conn, "enable_items", { dest, names: msg.names || [] });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "set_stock" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          const qty = Math.max(0, parseInt(msg.qty, 10) || 0);
          const threshold = Math.max(0, parseInt(msg.threshold, 10) || 0);
          this.inventory[dest][msg.name] = { qty, threshold };
          this.log(conn, "set_stock", { dest, name: msg.name, qty, threshold });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "clear_stock" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          delete this.inventory[dest][msg.name];
          this.log(conn, "clear_stock", { dest, name: msg.name });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "new_order") {
          const table = msg.table;
          if (!this.openTables[table]) {
            this.openTables[table] = { openedAt: Date.now() };
          }

          const isAvailable = (dest, name) => !this.unavailable[dest + "|" + name];
          const rawKitchen = msg.kitchenItems || [];
          const rawBar = msg.barItems || [];
          const kitchenItems = rawKitchen.filter(i => isAvailable("kitchen", i.name));
          const barItems = rawBar.filter(i => isAvailable("bar", i.name));
          const removed = [
            ...rawKitchen.filter(i => !isAvailable("kitchen", i.name)).map(i => i.name),
            ...rawBar.filter(i => !isAvailable("bar", i.name)).map(i => i.name),
          ];

          const order = {
            id: crypto.randomUUID(),
            table,
            createdAt: Date.now(),
            waiterName: conn.staffName || "",
            kitchenItems,
            barItems,
            kitchenStatus: kitchenItems.length ? "pending" : "none",
            barStatus: barItems.length ? "pending" : "none",
            kitchenAcceptedAt: null,
            kitchenReadyAt: null,
            barAcceptedAt: null,
            barReadyAt: null,
            cookName: null,
            bartenderName: null,
          };
          this.orders.push(order);
          this.log(conn, "new_order", { table, kitchenCount: kitchenItems.length, barCount: barItems.length });
          await this.persist();
          this.broadcast();
          if (order.kitchenStatus === "pending") this.notify("cook", { table: order.table, orderId: order.id });
          if (order.barStatus === "pending") this.notify("bartender", { table: order.table, orderId: order.id });
          if (removed.length) this.sendTo(conn, { type: "items_removed", names: removed });
        }

        if (msg.type === "kitchen_accept") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order && order.kitchenStatus === "pending") {
            order.kitchenStatus = "accepted";
            order.kitchenAcceptedAt = Date.now();
            order.cookName = conn.staffName || "";
            this.log(conn, "kitchen_accept", { table: order.table });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "bar_accept") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order && order.barStatus === "pending") {
            order.barStatus = "accepted";
            order.barAcceptedAt = Date.now();
            order.bartenderName = conn.staffName || "";
            this.log(conn, "bar_accept", { table: order.table });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "kitchen_ready") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            order.kitchenStatus = "ready";
            order.kitchenReadyAt = Date.now();
            this.consumeStock("kitchen", order.kitchenItems, "cook");
            this.log(conn, "kitchen_ready", { table: order.table });
            await this.persist();
            this.broadcast();
            this.notify("waiter", { table: order.table, orderId: order.id, part: "kitchen" });
          }
        }

        if (msg.type === "bar_ready") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            order.barStatus = "ready";
            order.barReadyAt = Date.now();
            this.consumeStock("bar", order.barItems, "bartender");
            this.log(conn, "bar_ready", { table: order.table });
            await this.persist();
            this.broadcast();
            this.notify("waiter", { table: order.table, orderId: order.id, part: "bar" });
          }
        }

        if (msg.type === "cancel_order") {
          const order = this.orders.find(o => o.id === msg.orderId);
          this.orders = this.orders.filter(o => o.id !== msg.orderId);
          if (order) this.log(conn, "cancel_order", { table: order.table });
          await this.persist();
          this.broadcast();
          if (order) {
            if (order.kitchenItems.length) this.notify("cook", { table: order.table, orderId: order.id, cancelled: true });
            if (order.barItems.length) this.notify("bartender", { table: order.table, orderId: order.id, cancelled: true });
          }
        }

        if (msg.type === "served") {
          const order = this.orders.find(o => o.id === msg.orderId);
          this.orders = this.orders.filter(o => o.id !== msg.orderId);
          if (order) {
            order.servedAt = Date.now();
            order.total = lineTotal(order.kitchenItems) + lineTotal(order.barItems);
            this.history.push(order);
            if (this.history.length > HISTORY_LIMIT) this.history = this.history.slice(-HISTORY_LIMIT);
            this.log(conn, "served", { table: order.table });
          }
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "close_table") {
          const table = msg.table;
          const session = this.openTables[table];
          if (!session) {
            this.sendTo(conn, { type: "close_error", table, reason: "not_open" });
            return;
          }
          const pending = this.orders.filter(o => o.table === table);
          if (pending.length > 0) {
            this.sendTo(conn, { type: "close_error", table, reason: "pending_items" });
            return;
          }

          const rounds = this.history.filter(h => h.table === table && h.servedAt >= session.openedAt);
          const merged = {}; // key: dest|name -> {name, qty, price}
          rounds.forEach(r => {
            (r.kitchenItems || []).forEach(i => {
              const key = "kitchen|" + i.name;
              if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price };
              merged[key].qty += i.qty;
            });
            (r.barItems || []).forEach(i => {
              const key = "bar|" + i.name;
              if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price };
              merged[key].qty += i.qty;
            });
          });
          const items = Object.values(merged);
          const subtotal = items.reduce((s, i) => s + parsePrice(i.price) * i.qty, 0);
          const service = Math.round(subtotal * SERVICE_RATE);
          const total = subtotal + service;

          const receipt = {
            id: crypto.randomUUID(),
            table,
            openedAt: session.openedAt,
            closedAt: Date.now(),
            closedBy: conn.staffName || "",
            items,
            subtotal,
            service,
            total,
          };

          delete this.openTables[table];
          this.closedTables.push(receipt);
          if (this.closedTables.length > CLOSED_TABLES_LIMIT) this.closedTables = this.closedTables.slice(-CLOSED_TABLES_LIMIT);
          this.log(conn, "close_table", { table, total });

          await this.persist();
          this.broadcast();
          this.sendTo(conn, { type: "table_closed", receipt });
        }
      });

      server.addEventListener("close", () => this.sockets.delete(conn));
      server.addEventListener("error", () => this.sockets.delete(conn));

      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("GO pub order board", { status: 200 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "*",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    if (url.pathname === "/ws") {
      const id = env.ORDER_BOARD.idFromName("main");
      const stub = env.ORDER_BOARD.get(id);
      return stub.fetch(request);
    }

    return new Response("GO pub order board is running.", { headers: cors });
  },
};
