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

// Combo part lines (price 0, routed to the other station) are never billed —
// the combo line itself carries the price.
function billable(items) {
  return (items || []).filter(i => !i.component);
}

const IMG_KEY_RE = /^[a-z0-9-]+\/[a-z0-9-]+\.(jpg|png|webp)$/;
const MAX_IMG_BYTES = 700 * 1024; // well under the 2 MB per-value limit of SQLite-backed DO storage

function dataUrlToBytes(dataUrl) {
  const m = /^data:(image\/(jpeg|png|webp));base64,([A-Za-z0-9+/=]+)$/.exec(String(dataUrl || ""));
  if (!m) return null;
  const bin = atob(m[3]);
  if (bin.length > MAX_IMG_BYTES) return null;
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return { bytes, type: m[1], ext: m[2] === "jpeg" ? "jpg" : m[2] };
}

// Sets one translation of a {ru, kk, en} field: text -> set, "" -> remove
// (clients then fall back to ru), undefined -> leave as is.
function setLang(obj, lang, value, max) {
  if (value === undefined || !obj) return;
  const v = String(value || "").trim().slice(0, max);
  if (v) obj[lang] = v; else delete obj[lang];
}

function sanitizeParts(raw, homeDest) {
  if (!Array.isArray(raw)) return [];
  return raw.slice(0, 10).map(p => ({
    dest: p && (p.dest === "kitchen" || p.dest === "bar") ? p.dest : (homeDest === "bar" ? "kitchen" : "bar"),
    qty: Math.min(50, Math.max(1, parseInt(p && p.qty, 10) || 1)),
    name: String((p && p.name) || "").trim().slice(0, 60),
    ...(p && p.note ? { note: String(p.note).trim().slice(0, 60) } : {}),
  })).filter(p => p.name);
}

function lineTotal(items) {
  return (items || []).reduce((sum, i) => sum + parsePrice(i.price) * (i.qty || 1), 0);
}

const DEFAULT_MENU = {"bar":[{"title":{"ru":"Бутылочное пиво","en":"Bottled Beer","kk":"Бөтелкедегі сыра"},"items":[{"name":{"ru":"Heineken 0.5","en":"Heineken 0.5","kk":"Heineken 0.5"},"price":"2 250","id":"dbebb8be-f013-43b4-b706-a0c4f0fc4a95"},{"name":{"ru":"Heineken безалкогольное 0.5","en":"Heineken Non-Alcoholic 0.5","kk":"Heineken алкогольсіз 0.5"},"price":"2 450","id":"70f85f0d-0971-48aa-839f-8ea2b977bbf0"},{"name":{"ru":"Kronenbourg 1664 0.5L","en":"Kronenbourg 1664 0.5L","kk":"Kronenbourg 1664 0.5L"},"price":"2 350","id":"9f137830-813d-4f7e-85ed-eff38bf3ecc3"},{"name":{"ru":"Corona Extra 0.35L","en":"Corona Extra 0.35L","kk":"Corona Extra 0.35L"},"price":"3 550","id":"a5e9bbaa-3626-4f28-ac5e-172a0e3a2fd2"},{"name":{"ru":"Krusovice Svetle 0.45L","en":"Krusovice Svetle 0.45L","kk":"Krusovice Svetle 0.45L"},"price":"1 590","id":"2aca41a9-87b0-442f-9947-8c22ad9719c2"},{"name":{"ru":"Krusovice Cerne 0.45L","en":"Krusovice Cerne 0.45L","kk":"Krusovice Cerne 0.45L"},"price":"1 590","id":"7d9174cc-c678-40fd-9130-63b8c22339de"},{"name":{"ru":"Жигули Барное безалкогольное 0.45L","en":"Zhiguli Barnoye Non-Alcoholic 0.45L","kk":"Жигули Барное алкогольсіз 0.45L"},"price":"1 450","id":"084c76b7-6726-4ec5-b6aa-f95a8a21207e"}],"id":"4944d2f8-0d75-4187-ab00-899dcf0d7460"},{"title":{"ru":"Разливное пиво","en":"Draft Beer","kk":"Құйма сыра"},"note":{"ru":"цена за 0.5 л / за 3 л","en":"price per 0.5 L / 3 L","kk":"0.5 л / 3 л бағасы"},"items":[{"name":{"ru":"Пражское","en":"Prague","kk":"Прага"},"price":"1 450","price2":"7 990","id":"daddf3cb-267d-4aee-8b53-c83bb92d273a"},{"name":{"ru":"Баварское нефильтрованное","en":"Bavarian Unfiltered","kk":"Бавария сүзілмеген"},"price":"1 750","price2":"9 990","id":"dd7c1246-1315-429e-9dd1-5a39ad92fd35"},{"name":{"ru":"Budweiser Budvar","en":"Budweiser Budvar","kk":"Budweiser Budvar"},"price":"3 590","price2":"20 590","id":"ff5b8fed-13f2-47f8-aeb1-26e97e6d6c73"},{"name":{"ru":"Guinness","en":"Guinness","kk":"Guinness"},"price":"3 990","id":"e9e4077a-bf99-454e-a875-86d0aa897105"}],"id":"736c1525-3f6f-45c8-aec0-9eb28c111a2f"},{"title":{"ru":"Закуски к пиву","en":"Beer Snacks","kk":"Сыраға тіскебасар"},"items":[{"name":{"ru":"Арахис соленый","en":"Salted Peanuts","kk":"Тұзды жержаңғақ"},"price":"1 690","id":"0f519828-9351-4b7e-827c-0176a890c1c5"},{"name":{"ru":"Чечил","en":"Chechil Cheese","kk":"Шешіл ірімшігі"},"price":"1 990","id":"c0c4a079-886a-4930-9b06-e5e23f804cd4"},{"name":{"ru":"Чипсы","en":"Chips","kk":"Чипсы"},"price":"2 190","id":"8dafb8db-5ccc-42eb-9259-62be0b4e080e"},{"name":{"ru":"Фисташки соленые","en":"Salted Pistachios","kk":"Тұзды фисташка"},"price":"2 390","id":"d09b080c-d422-4049-a7f1-3e6359f21ade"},{"name":{"ru":"Курт","en":"Kurt","kk":"Құрт"},"price":"1 990","id":"3ac66536-9aec-4bd2-b614-54d99333feb8"}],"id":"54517935-df6c-4fdf-bb8d-450dee13c4f3"},{"title":{"ru":"Водка","en":"Vodka","kk":"Арақ"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Хортица","en":"Khortytsa","kk":"Хортица"},"price":"9 500","price2":"950","id":"cdd52d12-eb20-4e12-b2be-ccfb4a9f8555"},{"name":{"ru":"Бульбашъ особая","en":"Bulbash Osobaya","kk":"Бульбашъ особая"},"price":"9 800","price2":"980","id":"a2fe89b9-6aaa-484e-88d3-b8e6a050cb69"},{"name":{"ru":"Kyzyl Zhar Legend","en":"Kyzyl Zhar Legend","kk":"Kyzyl Zhar Legend"},"price":"9 900","price2":"990","id":"59bbbea7-7f0b-4fdc-aabb-3ea6da8fdb0e"},{"name":{"ru":"Romanov","en":"Romanov","kk":"Romanov"},"price":"11 500","price2":"1 150","id":"786f95ca-f2de-4733-8a39-b8256afc8941"},{"name":{"ru":"Tchaikovsky","en":"Tchaikovsky","kk":"Tchaikovsky"},"price":"12 900","price2":"1 290","id":"c8ac0587-f19a-403f-afca-856624a07902"},{"name":{"ru":"Smirnoff №21 RED","en":"Smirnoff №21 RED","kk":"Smirnoff №21 RED"},"price":"13 900","price2":"1 390","id":"eaa5a986-d9e2-4a6e-8366-e7775211e6c8"},{"name":{"ru":"Absolut Blue","en":"Absolut Blue","kk":"Absolut Blue"},"price":"19 500","price2":"1 950","id":"7a60935c-b76a-4cbc-87d4-f33cfe37f268"},{"name":{"ru":"Tito's","en":"Tito's","kk":"Tito's"},"price":"27 900","price2":"2 790","id":"5348b762-0d4e-459b-be77-b1fcde85308a"},{"name":{"ru":"Reyka","en":"Reyka","kk":"Reyka"},"price":"32 000","price2":"3 200","id":"8d763dd7-b3e4-4cd7-b7fa-48661dc276f3"},{"name":{"ru":"Grey Goose","en":"Grey Goose","kk":"Grey Goose"},"price":"37 000","price2":"3 700","id":"75eb9fb3-3e7e-4adc-9208-49fc3185a17b"},{"name":{"ru":"Belvedere","en":"Belvedere","kk":"Belvedere"},"price":"41 000","price2":"4 100","id":"a3a14860-9119-4085-8c6e-8b48bb3f5289"}],"id":"6d8c237c-9ef6-4867-ac3e-178c8768fca7"},{"title":{"ru":"Виски","en":"Whisky","kk":"Виски"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Ballantine's","en":"Ballantine's","kk":"Ballantine's"},"price":"23 500","price2":"2 350","id":"b2e3c78b-7fdc-4d9b-a0d1-64c1d1cb4245"},{"name":{"ru":"Red Label","en":"Red Label","kk":"Red Label"},"price":"24 500","price2":"2 450","id":"cdade56d-b2fd-44fc-b1d9-6be8cd9da696"},{"name":{"ru":"Jameson","en":"Jameson","kk":"Jameson"},"price":"26 000","price2":"2 600","id":"93c19a2f-9967-4a8c-bc24-fa5652d3cf4b"},{"name":{"ru":"Tullamore D.E.W.","en":"Tullamore D.E.W.","kk":"Tullamore D.E.W."},"price":"28 000","price2":"2 800","id":"14989302-691d-462d-8978-c1ba6c1be536"},{"name":{"ru":"Jack Daniels","en":"Jack Daniels","kk":"Jack Daniels"},"price":"33 500","price2":"3 500","id":"ad5ce8e2-5f07-4e2d-a50b-f97e3d5afcca"},{"name":{"ru":"Monkey Shoulder","en":"Monkey Shoulder","kk":"Monkey Shoulder"},"price":"39 500","price2":"3 900","id":"cd893e80-294e-497a-a9e3-4d1756a04a0b"},{"name":{"ru":"Chivas 12 y.o.","en":"Chivas 12 y.o.","kk":"Chivas 12 y.o."},"price":"41 000","price2":"4 100","id":"ac532d86-f1a0-4e0a-bd14-26395b41fb78"}],"id":"ba74596a-db64-4426-83e4-195555c14652"},{"title":{"ru":"Коньяк","en":"Cognac","kk":"Коньяк"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Казахстан 3 звезды","en":"Kazakhstan 3 Stars","kk":"Қазақстан 3 жұлдыз"},"price":"12 500","price2":"1 250","id":"9195ca37-af37-46fa-81d1-18ecfe968db3"},{"name":{"ru":"Казахстан 5 звезд","en":"Kazakhstan 5 Stars","kk":"Қазақстан 5 жұлдыз"},"price":"15 500","price2":"1 500","id":"98e52f32-7aa1-4720-9e3f-1ddcfde413db"},{"name":{"ru":"Арарат 3 звезды","en":"Ararat 3 Stars","kk":"Арарат 3 жұлдыз"},"price":"17 500","price2":"1 750","id":"cdbb1063-680d-4bfd-b308-9ba3f4e03abd"},{"name":{"ru":"Арарат 5 звезд","en":"Ararat 5 Stars","kk":"Арарат 5 жұлдыз"},"price":"20 000","price2":"2 000","id":"df059af1-3b52-49ab-9a14-787a6c83717b"},{"name":{"ru":"Hennessy V.S.","en":"Hennessy V.S.","kk":"Hennessy V.S."},"price":"45 000","price2":"4 500","id":"0683cbd2-ffe8-40e4-984d-75eadae415bc"}],"id":"d23b2b8d-7f5a-4d86-85da-35c08af3a9bf"},{"title":{"ru":"Ром","en":"Rum","kk":"Ром"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Oakheart","en":"Oakheart","kk":"Oakheart"},"price":"20 000","price2":"2 000","id":"6dc68c6c-fe20-4013-acb6-799b6f1597be"}],"id":"e31d38c3-1d1f-489d-b4a7-9b2fc09842f1"},{"title":{"ru":"Текила","en":"Tequila","kk":"Текила"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Olmeca","en":"Olmeca","kk":"Olmeca"},"price":"21 500","price2":"2 150","id":"75021b5f-9bd4-40d3-a2b0-5bf0286aa43b"}],"id":"b653e02c-c9f0-44f7-8bbc-d3b7ef0eb7ee"},{"title":{"ru":"Джин","en":"Gin","kk":"Джин"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Gordon's","en":"Gordon's","kk":"Gordon's"},"price":"18 500","price2":"1 850","id":"26efc7d2-6be2-469e-8754-b032c16bda38"},{"name":{"ru":"Beefeater","en":"Beefeater","kk":"Beefeater"},"price":"19 500","price2":"1 950","id":"9550e3d7-cba9-44cf-829c-6b00ac7d0210"},{"name":{"ru":"Bickens","en":"Bickens","kk":"Bickens"},"price":"20 500","price2":"2 050","id":"0380412d-82aa-4376-9740-69a7c9596791"}],"id":"18ea54c7-2b54-433b-8ee3-d003f371ccfc"},{"title":{"ru":"Ликеры","en":"Liqueurs","kk":"Ликерлер"},"note":{"ru":"цена за 0.5 л / за 50 мл","en":"price per 0.5 L / 50 ml","kk":"0.5 л / 50 мл бағасы"},"items":[{"name":{"ru":"Becherovka","en":"Becherovka","kk":"Becherovka"},"price":"18 500","price2":"1 850","id":"a811fecf-e411-47d4-b9f0-de51d759494f"},{"name":{"ru":"Baileys","en":"Baileys","kk":"Baileys"},"price":"19 500","price2":"1 950","id":"6018aa14-8e8b-4955-9a7a-ad0dfb1d316e"},{"name":{"ru":"Jägermeister","en":"Jägermeister","kk":"Jägermeister"},"price":"21 000","price2":"2 100","id":"c1a67470-bca3-447a-affb-bc10cf892535"}],"id":"e6e26a05-36b3-43b8-81d2-88af5a5cbbcd"},{"title":{"ru":"Аперитивы и вино","en":"Aperitifs & Wine","kk":"Аперитивтер мен шарап"},"items":[{"name":{"ru":"Martini","en":"Martini","kk":"Martini"},"price":"18 500","price2":"1 850","desc":{"ru":"1 л / 100 мл","en":"1 L / 100 ml","kk":"1 л / 100 мл"},"id":"8f5b10f4-a5fe-48bb-94db-809e0aef87e5"},{"name":{"ru":"Вино в ассортименте","en":"Assorted Wine","kk":"Түрлі шарап"},"price":"14 000","price2":"2 200","desc":{"ru":"0.7 л / 100 мл","en":"0.7 L / 100 ml","kk":"0.7 л / 100 мл"},"id":"9107506c-f6b0-40db-a0ca-3d408295bc66"}],"id":"1576be07-4858-4637-84bd-e0e0ee3f8d82"},{"title":{"ru":"Коктейли алкогольные","en":"Cocktails","kk":"Коктейльдер"},"items":[{"name":{"ru":"Aperol Spritz","en":"Aperol Spritz","kk":"Aperol Spritz"},"price":"3 190","id":"95c99a7e-54f9-4dde-bf06-75a11065c8d7"},{"name":{"ru":"Gin Tonic","en":"Gin Tonic","kk":"Gin Tonic"},"price":"2 790","id":"3bb61f9d-9c49-4f2b-95eb-719a2f509999"},{"name":{"ru":"Long Island","en":"Long Island","kk":"Long Island"},"price":"3 290","id":"41f29efe-8950-48e9-a085-620d2ecc3fbf"},{"name":{"ru":"Cuba Libre","en":"Cuba Libre","kk":"Cuba Libre"},"price":"2 590","id":"a5e2e81b-a6e5-40f7-b700-ad74ebdda6d3"},{"name":{"ru":"Mimosa","en":"Mimosa","kk":"Mimosa"},"price":"2 490","id":"a700f097-35fa-4d25-9478-63ff6c5887af"},{"name":{"ru":"Mojito","en":"Mojito","kk":"Mojito"},"price":"2 690","id":"761b1ab0-8ae4-422c-8003-7dcbec61a2c8"}],"id":"63dd85b7-83ae-419e-a2fb-dd722f0b650b"},{"title":{"ru":"Безалкогольные напитки","en":"Soft Drinks","kk":"Алкогольсіз сусындар"},"items":[{"name":{"ru":"Coca-Cola","en":"Coca-Cola","kk":"Coca-Cola"},"price":"1 990","price2":"1 490","price3":"1 690","desc":{"ru":"1 л / 0.5 л / 0.25 л","en":"1 L / 0.5 L / 0.25 L","kk":"1 л / 0.5 л / 0.25 л"},"id":"e9429f66-768a-4e71-a30d-352c1a0f51cd"},{"name":{"ru":"Минеральная вода","en":"Mineral Water","kk":"Минералды су"},"price":"1 150","price2":"890","desc":{"ru":"1 л / 0.5 л","en":"1 L / 0.5 L","kk":"1 л / 0.5 л"},"id":"a12f366a-439e-4e80-9d03-645bf044aaf4"},{"name":{"ru":"Сок","en":"Juice","kk":"Шырын"},"price":"2 390","id":"bf7cfd93-9870-427f-9d99-f95e9b994615"},{"name":{"ru":"Red Bull","en":"Red Bull","kk":"Red Bull"},"price":"2 450","id":"59603683-809e-418e-9b9d-f058bbf035bb"},{"name":{"ru":"Borjomi","en":"Borjomi","kk":"Borjomi"},"price":"2 550","id":"fe853949-445e-4abc-9690-0da9c90455ea"},{"name":{"ru":"Тоник Schweppes","en":"Schweppes Tonic","kk":"Schweppes тоник"},"price":"2 300","id":"856bc6d5-917b-4d16-977f-af48ea207cb0"},{"name":{"ru":"Лимонады","en":"Lemonades","kk":"Лимонадтар"},"price":"2 790","id":"20f28af7-99a9-4ea9-a8fd-b8f69072b7ae"},{"name":{"ru":"Морс клюквенный","en":"Cranberry Mors","kk":"Мүкжидек морсы"},"price":"2 590","id":"f298ff37-1ad2-49ff-b47f-cf16540509a8"}],"id":"c2d6c4fd-b06b-4f57-a4cd-a9fabab01442"},{"title":{"ru":"Чай","en":"Tea","kk":"Шай"},"items":[{"name":{"ru":"Чай в ассортименте (чашка)","en":"Assorted Tea (cup)","kk":"Түрлі шай (кесе)"},"price":"800","id":"95c4fe9a-91f4-4a2e-a0be-073442778f8a"},{"name":{"ru":"Чай черный (чайник)","en":"Black Tea (pot)","kk":"Қара шай (шәйнек)"},"price":"1 450","id":"abbeabe2-5bea-4768-88ba-a3b2581359e9"},{"name":{"ru":"Чай зеленый (чайник)","en":"Green Tea (pot)","kk":"Жасыл шай (шәйнек)"},"price":"1 650","id":"bdcecb78-c0c0-4487-885b-0b59499f3576"},{"name":{"ru":"Чай ташкентский (чайник)","en":"Tashkent Tea (pot)","kk":"Ташкент шайы (шәйнек)"},"price":"2 190","id":"7607d91f-14f1-4fb5-9e06-f65c2a7f4709"},{"name":{"ru":"Чай имбирный (чайник)","en":"Ginger Tea (pot)","kk":"Зімбір шайы (шәйнек)"},"price":"2 590","id":"2797bedc-f5d6-404c-9b0d-164230d7a056"},{"name":{"ru":"Чай облепиховый (чайник)","en":"Sea Buckthorn Tea (pot)","kk":"Шырғанақ шайы (шәйнек)"},"price":"2 690","id":"498e34a3-e6f3-4593-8e17-11b606f5c293"},{"name":{"ru":"Чай ягодный (чайник)","en":"Berry Tea (pot)","kk":"Жидек шайы (шәйнек)"},"price":"2 790","id":"71679e10-cb25-462e-b3bd-09bba08148ea"},{"name":{"ru":"Чай мараканский (чайник)","en":"Moroccan Tea (pot)","kk":"Мароккан шайы (шәйнек)"},"price":"2 990","id":"3f27a9a2-bc27-427f-91e2-ff3d1720d434"}],"id":"180213f2-95d7-461c-af69-38cca1ac4275"},{"title":{"ru":"Кофе","en":"Coffee","kk":"Кофе"},"items":[{"name":{"ru":"Jacobs (чашка)","en":"Jacobs (cup)","kk":"Jacobs (кесе)"},"price":"1 300","id":"9df61305-3bd7-44de-90f5-f11ad8ed7e50"}],"id":"b579ff6e-978c-46f1-8d30-4ded3875ba14"}],"kitchen":[{"title":{"ru":"Салаты","en":"Salads","kk":"Салаттар"},"items":[{"n":1,"name":{"ru":"Пекинский","en":"Peking Salad","kk":"Бейжің салаты"},"price":"3 690","desc":{"ru":"мясо, огурцы, болгарский перец, соус, лист салата","en":"meat, cucumber, bell pepper, sauce, lettuce","kk":"ет, қияр, бұрыш, тұздық, салат жапырағы"},"id":"1af21eed-3061-4a38-b545-fd5261e3e017"},{"n":2,"name":{"ru":"Греческий","en":"Greek Salad","kk":"Грек салаты"},"price":"3 590","desc":{"ru":"овощи, сыр фетакса, сливки","en":"vegetables, feta cheese, cream","kk":"көкөніс, фета ірімшігі, қаймақ"},"id":"644a9c19-3127-4ea5-8115-cfe401e6012c"},{"n":3,"name":{"ru":"Хрустящие баклажаны","en":"Crispy Eggplant","kk":"Қытырлақ баклажан"},"price":"3 890","desc":{"ru":"баклажан, помидор, соус","en":"eggplant, tomato, sauce","kk":"баклажан, қызанақ, тұздық"},"id":"d58ee10d-9494-4778-94b6-06320ef9a786"},{"n":4,"name":{"ru":"Теплый из требухи с овощами","en":"Warm Tripe with Vegetables","kk":"Көкөністі жылы қарын салаты"},"price":"3 890","id":"2c3140f4-64d5-4f82-b471-077d17851b11"},{"n":5,"name":{"ru":"Салат GO","en":"GO Salad","kk":"GO салаты"},"price":"3 890","desc":{"ru":"курица с апельсином, грибы, помидоры","en":"chicken with orange, mushrooms, tomatoes","kk":"тауық еті, апельсин, саңырауқұлақ, қызанақ"},"id":"ba786292-8f9b-4007-b3ee-54184e54eea9"}],"id":"d23c8bf0-76ba-45fc-ac13-d0db49d5a60e"},{"title":{"ru":"Холодные закуски","en":"Cold Appetizers","kk":"Салқын тағамдар"},"items":[{"n":1,"name":{"ru":"Ассорти под водочку","en":"Vodka Platter","kk":"Арақ ассорти"},"price":"3 990","desc":{"ru":"скумбрия, сельдь, долма, соленые огурчики","en":"mackerel, herring, dolma, pickles","kk":"скумбрия, майшабақ, толма, тұздалған қияр"},"id":"37fb50e2-47eb-4217-854a-7b4864552b01"},{"n":2,"name":{"ru":"Соленья","en":"Pickles","kk":"Тұздықтар"},"price":"3 790","desc":{"ru":"квашеная капуста, корнишоны, маринованные черри, оливки","en":"sauerkraut, gherkins, pickled cherry tomatoes, olives","kk":"ашытылған қырыққабат, қиярша, маринадталған қызанақ, зәйтүн"},"id":"a1d82c27-3abd-44d6-b4fd-db55bbdf8328"},{"n":3,"name":{"ru":"Кавказская нарезка","en":"Caucasian Platter","kk":"Кавказ асаны"},"price":"3 890","desc":{"ru":"огурцы, помидоры, болгарский перец, сыр Фета, зелень","en":"cucumber, tomato, bell pepper, feta cheese, herbs","kk":"қияр, қызанақ, бұрыш, фета ірімшігі, көк жиек"},"id":"215f0a6a-e359-48f9-8518-efc1f4f992f2"},{"n":4,"name":{"ru":"Мясная нарезка","en":"Meat Platter","kk":"Ет асаны"},"price":"6 590","desc":{"ru":"шужук, казы, мясо","en":"shuzhuk, kazy, meat","kk":"шұжық, қазы, ет"},"id":"fb246cad-7c4a-4923-aefd-c0a2a2080042"},{"n":5,"name":{"ru":"Холодец по-домашнему","en":"Homemade Aspic","kk":"Үй жасаған холодец"},"price":"3 650","id":"cea3c030-ef47-4925-a4e5-7ea0b4012980"},{"n":6,"name":{"ru":"Мужская закуска","en":"Hearty Platter","kk":"Ер-азамат тіскебасары"},"price":"6 590","desc":{"ru":"квашеная капуста, огурчики маринованные, курдючок баранины, холодец, горчица, черный хлеб","en":"sauerkraut, pickled cucumbers, lamb fat tail, aspic, mustard, black bread","kk":"ашытылған қырыққабат, маринадталған қияр, қой құйрығы, холодец, қыша, қара нан"},"id":"d449c176-1b30-47b5-b201-d5a5ae041f4c"},{"n":7,"name":{"ru":"Баклажаны в панировке","en":"Breaded Eggplant","kk":"Қамырланған баклажан"},"price":"3 790","id":"f137994f-d0d8-48ed-8549-3186927808cb"}],"id":"c8ba8167-a9f8-41cc-98ca-2a36ceabc492"},{"title":{"ru":"Горячие закуски","en":"Hot Appetizers","kk":"Ыстық тіскебасарлар"},"items":[{"n":1,"name":{"ru":"Крылья BBQ","en":"BBQ Wings","kk":"BBQ қанаттары"},"price":"3 590","id":"1ff7e573-2d64-4f57-98da-f71b4e2efc62"},{"n":2,"name":{"ru":"Сырные палочки","en":"Cheese Sticks","kk":"Ірімшік таяқшалары"},"price":"3 190","id":"ec24c8dc-f222-48a8-8036-ef666839075e"},{"n":3,"name":{"ru":"Нагетсы","en":"Nuggets","kk":"Наггетс"},"price":"2 890","id":"419e9c7a-f559-41d1-a03a-d891732b1219"},{"n":4,"name":{"ru":"Луковые кольца","en":"Onion Rings","kk":"Пияз сақиналары"},"price":"2 690","id":"e9f70644-d112-4b4e-a1a0-f77d6195f3f8"},{"n":5,"name":{"ru":"Гренки","en":"Croutons","kk":"Қытырлақ нан"},"price":"1 990","id":"b8397162-7cb3-461e-ab72-cdb3f0928650"},{"n":6,"name":{"ru":"Бараньи семечки","en":"Lamb Testicles","kk":"Қошқар жұмыртқасы"},"price":"3 950","id":"0fabb966-f2ca-4577-b999-69daa8813668"},{"n":7,"name":{"ru":"Жареные пельмени","en":"Fried Pelmeni","kk":"Қуырылған пельмень"},"price":"3 290","id":"f854a238-ad86-419f-b88f-d36c78a53a1f"},{"n":8,"name":{"ru":"Мини чебуречки","en":"Mini Chebureki","kk":"Мини шелпек"},"price":"2 690","id":"725628b8-2721-4aca-912d-5c02df58b04c"},{"n":9,"name":{"ru":"Печень с курдючком","en":"Liver with Fat Tail","kk":"Құйрықпен бауыр"},"price":"3 790","id":"369a7d89-e849-4531-9951-fe2115416258"}],"id":"29998c2c-a8d7-4bea-8bd5-eb3966b17bbd"},{"title":{"ru":"Супы","en":"Soups","kk":"Сорпалар"},"items":[{"n":1,"name":{"ru":"Пельмени домашние","en":"Homemade Pelmeni","kk":"Үй пельмені"},"price":"2 790","id":"57c38717-2894-4ba8-87f1-35f554957d79"},{"n":2,"name":{"ru":"Лапша домашняя","en":"Homemade Noodle Soup","kk":"Үй кеспесі"},"price":"2 290","id":"7eaae32b-cc6d-421f-b3d6-fcf51e3d91a5"},{"n":3,"name":{"ru":"Солянка","en":"Solyanka","kk":"Солянка"},"price":"2 690","id":"c7c0d8f6-702b-491c-8be7-e7f6d631c0ba"},{"n":4,"name":{"ru":"Том-Ям","en":"Tom Yum","kk":"Том-Ям"},"price":"3 590","id":"e76a8f7b-ab3e-46f9-892b-bf8d908f0edc"},{"n":5,"name":{"ru":"Рамён","en":"Ramen","kk":"Рамен"},"price":"2 990","id":"2e4bee77-8aed-4b12-96af-acfa304e69e8"}],"id":"67ec69cf-dcdd-4325-aa49-59ff53e05a45"},{"title":{"ru":"Море Go","en":"Sea GO","kk":"Теңіз GO"},"items":[{"n":1,"name":{"ru":"Креветки пивные","en":"Beer Shrimp","kk":"Сыралы асшаян"},"price":"4 290","id":"91b15800-ac05-4381-92d0-d4218890a1e1"},{"n":2,"name":{"ru":"Жаренные карасики с картофелем","en":"Fried Crucian Carp with Potatoes","kk":"Картоппен қуырылған табан балық"},"price":"3 590","id":"a3a8bcd7-c41f-4925-ac4d-918e8928ca07"},{"n":3,"name":{"ru":"Мойва","en":"Capelin","kk":"Мойва"},"price":"3 690","id":"2ae257a1-f473-4696-b291-ca55c43c33bf"},{"n":4,"name":{"ru":"Сазан на компанию с салатом по-домашнему","en":"Whole Carp to Share with Homemade Salad","kk":"Топқа арналған сазан, үй салаты"},"price":"16 590","id":"bbbbb318-7f2a-4096-b9ff-190cbe64a0e1"}],"id":"ec4aa39b-3667-458a-abda-39ae3e9bea85"},{"title":{"ru":"Пивной пир","en":"Beer Feast","kk":"Сыра мерекесі"},"note":{"ru":"сеты для компании, с пивом","en":"sharing sets with beer","kk":"топқа арналған, сырамен жинақтар"},"items":[{"n":1,"name":{"ru":"Рыбный сет + Пражское","en":"Fish Set + Prague Beer","kk":"Балық жинағы + Прага сырасы"},"price":"29 990","desc":{"ru":"сазан, мойва, караси, креветки, луковые кольца, картофельные дольки","en":"carp, capelin, crucian carp, shrimp, onion rings, potato wedges","kk":"сазан, мойва, табан балық, асшаян, пияз сақинасы, картоп бөлшектері"},"id":"0641fe27-7a8e-4d66-9289-a83d502d2c7a"},{"n":2,"name":{"ru":"Куриный сет + Пражское","en":"Chicken Set + Prague Beer","kk":"Тауық жинағы + Прага сырасы"},"price":"24 990","desc":{"ru":"BBQ, наггетсы, kfc, луковые кольца, сырные палочки, фри","en":"BBQ wings, nuggets, kfc-style chicken, onion rings, cheese sticks, fries","kk":"BBQ қанат, наггетс, kfc тауық, пияз сақинасы, ірімшік таяқша, фри"},"id":"0aa055dd-2b8b-46e4-a976-78191b5ecf21"},{"n":3,"name":{"ru":"Мясная доска + Баварское нефильтрованное","en":"Meat Board + Bavarian Unfiltered","kk":"Ет тақтасы + Бавария сырасы"},"price":"31 990","desc":{"ru":"рибай, тибон, стейк куриный, утка, колбасы, цыпленок табака, овощи гриль","en":"ribeye, t-bone, chicken steak, duck, sausages, chicken tabaka, grilled vegetables","kk":"рибай, ти-бон, тауық стейгі, үйрек, шұжық, тәбака тауығы, грильдегі көкөніс"},"id":"fa8877c6-0a1e-4a5d-a5bf-464289e06da3"},{"n":4,"name":{"ru":"Улов рыбака + Баварское нефильтрованное","en":"Fisherman's Catch + Bavarian Unfiltered","kk":"Балықшы уловы + Бавария сырасы"},"price":"21 990","desc":{"ru":"копченая, вяленая, сушеная рыбка","en":"smoked, cured, dried fish","kk":"ыстатылған, кептірілген балық"},"id":"38356b34-7454-4e3c-baeb-8726554a7324"},{"n":5,"name":{"ru":"Шашлычный сет GO + Пражское или Баварское нефильтрованное","en":"GO Skewer Set + Prague or Bavarian Unfiltered","kk":"GO шашлық жинағы + Прага/Бавария сырасы"},"price":"35 990","desc":{"ru":"2 баранины, 2 утки, 2 филе, 2 крыла, грибы, овощи, картофельные дольки","en":"2 lamb, 2 duck, 2 fillet, 2 wings, mushrooms, vegetables, potato wedges","kk":"2 қой еті, 2 үйрек, 2 филе, 2 қанат, саңырауқұлақ, көкөніс, картоп бөлшектері"},"id":"49e1d8a0-acf9-44ba-ba40-b5bb62e16f46"}],"id":"a94d59c0-05b7-41a2-9903-f738beae7e74"},{"title":{"ru":"Street food","en":"Street Food","kk":"Көше тағамдары"},"items":[{"n":1,"name":{"ru":"Beef Burger","en":"Beef Burger","kk":"Бифбургер"},"price":"3 890","id":"5c6ed7e7-4164-4bbe-9d29-ca3303697e73"},{"n":2,"name":{"ru":"Chicken Burger","en":"Chicken Burger","kk":"Тауықбургер"},"price":"3 690","id":"cfdd4dec-2f8e-447b-9520-c5b9137df4d2"},{"n":3,"name":{"ru":"Шаурма с курицей","en":"Chicken Shawarma","kk":"Тауық шаурмасы"},"price":"3 590","id":"6fbe2d66-3898-4db1-bd34-c9dcbcfd169f"},{"n":4,"name":{"ru":"Куриный пирог GO","en":"GO Chicken Pie","kk":"GO тауық пирогы"},"price":"5 390","id":"480e06a2-34e7-4a3a-9ffc-c91f78e04c68"},{"n":5,"name":{"ru":"Баскет с фри","en":"Basket with Fries","kk":"Фримен себет"},"price":"9 290","id":"39e075d0-6303-46a8-b66c-c19065416f80"}],"id":"3b9249f0-97c3-44cb-baea-bbbcc5a190bc"},{"title":{"ru":"Пицца","en":"Pizza","kk":"Пицца"},"items":[{"n":1,"name":{"ru":"Курица с грибами","en":"Chicken & Mushroom","kk":"Тауық пен саңырауқұлақ"},"price":"3 890","desc":{"ru":"моцарелла, филе курицы, соус, шампиньоны","en":"mozzarella, chicken fillet, sauce, mushrooms","kk":"моцарелла, тауық филесі, тұздық, шампиньон"},"id":"519ab09c-92d1-40b8-8b3b-9f5d97987231"},{"n":2,"name":{"ru":"Мексикано","en":"Mexicano","kk":"Мексикано"},"price":"3 990","desc":{"ru":"моцарелла, мясо говядины, болгарский перец, халапеньо","en":"mozzarella, beef, bell pepper, jalapeño","kk":"моцарелла, сиыр еті, бұрыш, халапеньо"},"id":"61d7bbcb-d8f0-4c03-81b5-ec217e291a1d"},{"n":3,"name":{"ru":"Пицца 4 сезона","en":"Four Seasons","kk":"Төрт маусым"},"price":"3 890","desc":{"ru":"моцарелла, салями, грибы, мясо говядины, помидоры","en":"mozzarella, salami, mushrooms, beef, tomatoes","kk":"моцарелла, салями, саңырауқұлақ, сиыр еті, қызанақ"},"id":"7005c72f-b3fd-4b8e-9a58-fce5f60c29c1"},{"n":4,"name":{"ru":"Маргарита","en":"Margherita","kk":"Маргарита"},"price":"3 190","desc":{"ru":"моцарелла, помидоры, соус","en":"mozzarella, tomatoes, sauce","kk":"моцарелла, қызанақ, тұздық"},"id":"6c2ef96f-20a9-4396-a7cd-746f6fc58385"},{"n":5,"name":{"ru":"Пепперони","en":"Pepperoni","kk":"Пепперони"},"price":"3 890","desc":{"ru":"моцарелла, копченая колбаса","en":"mozzarella, smoked sausage","kk":"моцарелла, ысталған шұжық"},"id":"bcf2c22b-0b78-426b-bf7e-cf975e853782"},{"n":6,"name":{"ru":"Сырная","en":"Four Cheese","kk":"Ірімшікті"},"price":"3 990","id":"def5e6eb-11b2-4147-a2f8-e0d575ea693d"}],"id":"c4dee86b-eee9-4dc2-9a84-adb369e05200"},{"title":{"ru":"Паста","en":"Pasta","kk":"Паста"},"items":[{"n":1,"name":{"ru":"Альфредо с фетучини","en":"Fettuccine Alfredo","kk":"Феттучини Альфредо"},"price":"3 790","id":"0f662730-685e-4559-a007-456ed63155e8"},{"n":2,"name":{"ru":"Болоньезе","en":"Bolognese","kk":"Болоньезе"},"price":"3 790","id":"39eebd3e-dac7-49af-a4a9-74d83122a841"}],"id":"f3727e50-6725-4106-aa33-962b5d8307f0"},{"title":{"ru":"Шашлык","en":"Skewers","kk":"Шашлық"},"items":[{"n":1,"name":{"ru":"Баранина","en":"Lamb","kk":"Қой еті"},"price":"5 000","id":"e15313f2-dcf4-4e57-9a37-fd1c5a5d6083"},{"n":2,"name":{"ru":"Телятина","en":"Veal","kk":"Бұзау еті"},"price":"5 000","id":"4ceb1497-8147-4923-862b-24c2edbc2301"},{"n":3,"name":{"ru":"Утка","en":"Duck","kk":"Үйрек"},"price":"4 590","id":"b86811d9-37b8-43bb-9f6c-fd5b58122eba"},{"n":4,"name":{"ru":"Крылышки","en":"Wings","kk":"Қанат"},"price":"3 990","id":"7fcd78f7-3a47-4140-8a5f-1bfb4f7ff143"},{"n":5,"name":{"ru":"Куриное филе","en":"Chicken Fillet","kk":"Тауық филесі"},"price":"3 750","id":"c76c96dc-90cb-47ff-8f3b-2f42ccf13838"},{"n":6,"name":{"ru":"Грибы","en":"Mushrooms","kk":"Саңырауқұлақ"},"price":"3 750","id":"6483e993-3bc6-4d21-abad-816d9274a927"},{"n":7,"name":{"ru":"Картофельный","en":"Potato","kk":"Картоп"},"price":"2 450","id":"0088891d-054a-468c-8716-568c52658946"},{"n":8,"name":{"ru":"Овощи гриль","en":"Grilled Vegetables","kk":"Грильдегі көкөніс"},"price":"2 890","id":"cccd924f-246c-4a4e-b9a4-abfc9c4e80a5"}],"id":"931d180c-1af4-44b1-a3ab-cb0258f86e55"},{"title":{"ru":"Горячие блюда","en":"Main Courses","kk":"Ыстық тағамдар"},"items":[{"n":1,"name":{"ru":"Стейк Рибай","en":"Ribeye Steak","kk":"Рибай стейгі"},"price":"7 390","id":"54682a93-8624-413b-a782-17e0bf70fb47"},{"n":2,"name":{"ru":"Стейк Ти-бон","en":"T-Bone Steak","kk":"Ти-бон стейгі"},"price":"7 390","id":"341ec0a4-e2d6-4af9-8d91-05b8f47391e3"},{"n":3,"name":{"ru":"Микс на жаровне","en":"Grill Mix","kk":"Грильдегі ассорти"},"price":"4 490","id":"a872e8a0-e57c-4b7e-a751-9b74c575641c"},{"n":4,"name":{"ru":"Мясо по-мексикански","en":"Mexican-Style Meat","kk":"Мексикалық ет"},"price":"4 890","id":"89025a3d-62b1-48d6-b173-8f5375321eec"},{"n":5,"name":{"ru":"Медальоны в сливочно-грибном соусе","en":"Medallions in Creamy Mushroom Sauce","kk":"Қаймақ-саңырауқұлақ тұздығындағы медальон"},"price":"5 290","id":"1d430d2c-d563-4308-bef9-c8edffce4815"},{"n":6,"name":{"ru":"Куырдак","en":"Kuyrdak","kk":"Куырдақ"},"price":"5 290","id":"7ecbce9f-30f2-4997-918c-4cfead69a9bf"},{"n":7,"name":{"ru":"Курица с грибами в сливочном соусе с пюре","en":"Chicken & Mushrooms in Cream Sauce with Mash","kk":"Пюремен қаймақты тұздықтағы тауық"},"price":"4 190","id":"cf8d39c2-8ead-4a41-bd4d-aab944aac1ef"},{"n":8,"name":{"ru":"Говядина с шампиньонами и рисом","en":"Beef with Mushrooms and Rice","kk":"Күрішпен, шампиньонмен сиыр еті"},"price":"4 790","id":"80cae8fd-54b1-427f-802b-8fd349426fd4"}],"id":"aa99406b-c9c9-4a63-b654-c43de30474e9"},{"title":{"ru":"Сеты на компанию","en":"Sharing Sets","kk":"Топқа арналған жинақтар"},"items":[{"n":1,"name":{"ru":"Пивная доска №1","en":"Beer Board #1","kk":"Сыра тақтасы №1"},"price":"4 990","desc":{"ru":"чебуреки, гренки, редис, соусы","en":"chebureki, croutons, radish, sauces","kk":"шелпек, қытырлақ нан, шалғам, тұздықтар"},"id":"c02a4db8-3f2d-4964-934c-994e50d69869"},{"n":2,"name":{"ru":"Пивная доска №2","en":"Beer Board #2","kk":"Сыра тақтасы №2"},"price":"11 590","desc":{"ru":"пельмени жареные, сырные палочки, луковые кольца, гренки, соусы","en":"fried pelmeni, cheese sticks, onion rings, croutons, sauces","kk":"қуырылған пельмень, ірімшік таяқша, пияз сақинасы, қытырлақ нан, тұздықтар"},"id":"a4aec786-a2e0-4e9f-a01b-9a0f2ffad435"},{"n":3,"name":{"ru":"Пивная доска №3","en":"Beer Board #3","kk":"Сыра тақтасы №3"},"price":"14 490","desc":{"ru":"крылья BBQ, бараньи семечки, сырные палочки, наггетсы, соусы, орешки","en":"BBQ wings, lamb testicles, cheese sticks, nuggets, sauces, nuts","kk":"BBQ қанат, қошқар жұмыртқасы, ірімшік таяқша, наггетс, тұздықтар, жаңғақ"},"id":"02dac13c-1768-4d0d-8149-631a88a558dd"},{"n":4,"name":{"ru":"Ассорти колбасок, дольки, соусы","en":"Sausage Assortment, Wedges, Sauces","kk":"Шұжық ассортиі, картоп бөлшектері, тұздықтар"},"price":"12 690","id":"5d6969a0-a85d-4d21-8616-972cc3488fc8"},{"n":5,"name":{"ru":"Пивной микс","en":"Beer Mix","kk":"Сыра миксі"},"price":"7 690","desc":{"ru":"чечил, фисташки, орешки, чипсы, сухарики, курт","en":"chechil cheese, pistachios, nuts, chips, croutons, kurt","kk":"шешіл ірімшігі, фисташка, жаңғақ, чипсы, кептірілген нан, құрт"},"id":"85c13bac-de50-4bf9-9719-8b8ce18e4194"},{"n":6,"name":{"ru":"Ведро креветок","en":"Bucket of Shrimp","kk":"Асшаян шелегі"},"price":"14 990","id":"85d32b8a-561c-4164-b0f9-c23449a5df3b"},{"n":7,"name":{"ru":"Стейки 2+1","en":"Steaks 2+1","kk":"Стейктер 2+1"},"price":"16 990","id":"a35f15fa-2ca9-40cc-b746-bb0792fbed52"}],"id":"7ea252be-dc80-4a61-8c74-182211fce64e"},{"title":{"ru":"Гарнир","en":"Sides","kk":"Гарнирлер"},"items":[{"n":1,"name":{"ru":"Рис","en":"Rice","kk":"Күріш"},"price":"1 450","id":"dd96294b-116e-447b-a230-7e83996fb09e"},{"n":2,"name":{"ru":"Картофельный фри","en":"French Fries","kk":"Картоп фри"},"price":"1 500","id":"4caa6b23-690a-4c75-a45c-14912c818be7"},{"n":3,"name":{"ru":"Картофельные дольки","en":"Potato Wedges","kk":"Картоп бөлшектері"},"price":"1 550","id":"7367008b-bbdb-4701-9655-e02a9fb31a3c"},{"n":4,"name":{"ru":"Картошка по-домашнему","en":"Home-style Potatoes","kk":"Үй картобы"},"price":"2 790","id":"1851f45c-c488-45c4-a5f0-976af2464021"},{"n":5,"name":{"ru":"Соусы","en":"Sauces","kk":"Тұздықтар"},"price":"800","id":"482d7648-d65e-48d8-86b6-dddc8319cb9a"}],"id":"4434aa40-f629-4976-840c-c1d9b0cc6f55"},{"title":{"ru":"Дессерт","en":"Dessert","kk":"Десерттер"},"items":[{"n":1,"name":{"ru":"Фруктовая нарезка","en":"Fruit Platter","kk":"Жеміс тілімдері"},"price":"7 990","id":"450d093d-afb0-4a14-8cee-74ab3c87be77"},{"n":2,"name":{"ru":"Сладкий десерт","en":"Sweet Dessert","kk":"Тәтті десерт"},"price":"2 190","id":"d4272368-6a15-4466-8836-1e44e7ddcdec"},{"n":3,"name":{"ru":"Мороженое","en":"Ice Cream","kk":"Балмұздақ"},"price":"1 990","id":"92318b39-b88b-448e-b53b-ad88016e4797"},{"n":4,"name":{"ru":"Лимон","en":"Lemon","kk":"Лимон"},"price":"890","id":"bc38e363-4079-4281-97fe-58dc0d855b3d"},{"n":5,"name":{"ru":"Хлебная корзина","en":"Bread Basket","kk":"Нан себеті"},"price":"800","id":"9459e27c-7a0a-4ecd-9f3e-e7abeb75eaa0"},{"n":6,"name":{"ru":"Хлебная корзина 1/2","en":"Bread Basket (half)","kk":"Нан себеті 1/2"},"price":"400","id":"ec46aac7-ef2a-42a0-be25-2e19c7769932"}],"id":"85caf3f1-9e31-43c7-b36b-cb55f506fdd9"}]};

// Garage Music Bar's starting menu — imported from its kamiqr.com QR menu
// (garage-music-bar.kamiqr.com/menu/basic) on 2026-10-03. Only used the very
// first time the "garage-music-bar" Durable Object is reached; after that the
// manager edits it in manager.html like GO pub's.
// Photos: img/imgFull point at static files in the venue's Pages site (/menu-img/…),
// filled in by that repo's "Import kamiqr photos" GitHub Action.
const GARAGE_MENU = {"kitchen":[{"title":{"ru":"Акции","kk":"Жеңілдіктер","en":"Specials"},"note":{"ru":"Наличие акций уточняйте у официанта","kk":"Акциялардың бар-жоғын даяшыдан сұраңыз","en":"Ask your waiter which specials are available today"},"items":[{"name":{"ru":"Птичий микс + Водка Alpha 0,5 l","kk":"Құс етінен микс + Alpha арағы 0,5 л"},"price":"27 000","id":"7f292ca5-9d01-4808-a2c9-d7c2f2e32405","desc":{"ru":"обычная цена 32 000","kk":"бұрынғы бағасы 32 000"},"img":"/menu-img/7f292ca5-9d01-4808-a2c9-d7c2f2e32405-t.jpg","imgFull":"/menu-img/7f292ca5-9d01-4808-a2c9-d7c2f2e32405.jpg","parts":[{"dest":"bar","qty":1,"name":"Водка ALPHA 0,5 л (бутылка)"}]},{"name":{"ru":"Рыбное ассорти на компанию + белое Вино Алазанская Долина","kk":"Компанияға балық ассортиі + «Алазан аңғары» ақ шарабы"},"price":"34 990","id":"de8a3643-6f8b-4411-88fc-af7f522e4310","desc":{"ru":"обычная цена 40 800","kk":"бұрынғы бағасы 40 800"},"img":"/menu-img/de8a3643-6f8b-4411-88fc-af7f522e4310-t.jpg","imgFull":"/menu-img/de8a3643-6f8b-4411-88fc-af7f522e4310.jpg","parts":[{"dest":"bar","qty":1,"name":"Вино Тетри Алазанская долина бел. п/сл"}]},{"name":{"ru":"Вино Цинандали +винная тарелка","kk":"Цинандали шарабы + шарап тәрелкесі"},"price":"14 200","id":"b1128e05-bf1b-4986-b6a4-4bbaa2e92c95","img":"/menu-img/b1128e05-bf1b-4986-b6a4-4bbaa2e92c95-t.jpg","imgFull":"/menu-img/b1128e05-bf1b-4986-b6a4-4bbaa2e92c95.jpg","parts":[{"dest":"bar","qty":1,"name":"Цинандали Marani (Бел/ сух)"}]},{"name":{"ru":"Квиз сет + 4 пива","kk":"Квиз сеті + 4 сыра"},"price":"15 390","id":"24661b3e-702c-4d67-9211-032b8898410f","img":"/menu-img/24661b3e-702c-4d67-9211-032b8898410f-t.jpg","imgFull":"/menu-img/24661b3e-702c-4d67-9211-032b8898410f.jpg","parts":[{"dest":"bar","qty":4,"name":"Garage светлое","note":"0,5 л"}]},{"name":{"ru":"Килограмм креветок + 4 пива","kk":"Бір келі асшаян + 4 сыра"},"price":"17 900","id":"aac82b99-2bc6-4a8e-aaf4-54f1f370d503","img":"/menu-img/aac82b99-2bc6-4a8e-aaf4-54f1f370d503-t.jpg","imgFull":"/menu-img/aac82b99-2bc6-4a8e-aaf4-54f1f370d503.jpg","parts":[{"dest":"bar","qty":4,"name":"Garage светлое","note":"0,5 л"}]},{"name":{"ru":"Сет куриный, для большой компании","kk":"Үлкен компанияға арналған тауық етінен жасалған жиынтық"},"price":"25 500","id":"475f2716-26a4-4102-a763-8a6eacf7607f","desc":{"ru":"3,5 килограмма мяса птицы. (Курочка запеченная, утиное филе, куриное филе, куриные крылья, подается с картофелем фри и 2 видами соуса)","kk":"3,5 килограмм құс еті (қуырылған тауық еті, үйрек филесі, тауық филесі, тауық қанаттары, картоп фриімен және 2 түрлі тұздықпен беріледі)"},"img":"/menu-img/475f2716-26a4-4102-a763-8a6eacf7607f-t.jpg","imgFull":"/menu-img/475f2716-26a4-4102-a763-8a6eacf7607f.jpg"}],"id":"promo-kitchen"},{"title":{"ru":"Салаты","kk":"Салаттар","en":"Salads"},"items":[{"name":{"ru":"Салат Цезарь с куриным филе","kk":"Цезарь салаты"},"price":"3 200","id":"ca133f98-2130-339e-82bf-226c57b05189","desc":{"ru":"с цыпленком /тауық /Chicken. Классический салат с обжаренным куриным филе, салатом айсберг , помидорами черри, заправленный соусом цезарь, посыпается пармезаном","kk":"с цыпленком /тауық /Chicken. Айсберг, тауық еті, черри қызанақтары, кептірілген нан, бөдене жұмыртқасы, пармезан ірімшігі, Цезарь соусы."},"img":"/menu-img/ca133f98-2130-339e-82bf-226c57b05189-t.jpg","imgFull":"/menu-img/ca133f98-2130-339e-82bf-226c57b05189.jpg"},{"name":{"ru":"Салат Цезарь с креветками","kk":"Асшаянды Цезарь салаты"},"price":"3 500","id":"0dc563f3-83ff-431a-80ea-7ca8c2e33bf6","desc":{"ru":"Крупные креветки, сладкие помидоры черри, свежий салат айсберг в сочетании с оригинальным соусом цезарь и хрустящими сухариками","kk":"Ірі асшаяндар, тәтті черри қызанақтары, балғын айсберг салаты, түпнұсқа Цезарь тұздығы және қытырлақ кептірілген нан"},"img":"/menu-img/0dc563f3-83ff-431a-80ea-7ca8c2e33bf6-t.jpg","imgFull":"/menu-img/0dc563f3-83ff-431a-80ea-7ca8c2e33bf6.jpg"},{"name":{"ru":"Греческий салат","kk":"Грек салаты"},"price":"3 200","id":"b9cf5e45-7f9f-355d-8008-c2f19a26f365","desc":{"ru":"это микс из свежих овощей (огурца, помидора, болгарского перца, айсберга ) в сочетании с сыром «Фетакса»","kk":"Брынза қосылған жаңа піскен көкөністер, цитронет тұздығымен толтырылған, орегано қосылған."},"img":"/menu-img/b9cf5e45-7f9f-355d-8008-c2f19a26f365-t.jpg","imgFull":"/menu-img/b9cf5e45-7f9f-355d-8008-c2f19a26f365.jpg"},{"name":{"ru":"Руккола с креветками и помидорами черри","kk":"Асшаяндар мен черри қызанақтары қосылған руккола"},"price":"3 960","id":"2014ad95-2bdf-369a-8629-f38c8d9f326a","img":"/menu-img/2014ad95-2bdf-369a-8629-f38c8d9f326a-t.jpg","imgFull":"/menu-img/2014ad95-2bdf-369a-8629-f38c8d9f326a.jpg"},{"name":{"ru":"Теплый салат с говяжьим языком","kk":"Сиыр тілі қосылған жылы салат"},"price":"3 600","id":"e62eaaae-6a45-34f3-aa2f-09895deb073c","desc":{"ru":"(говяжий язык, свежие помидоры, цукини, капуста брокколи, салат айсберг, заправляется соусом от Шеф-повара)","kk":"(сиыр тілі, жаңа піскен қызанақ, цуккини, брокколи, айсберг салат, тұздықпен безендірілген аспазшыдан)"},"img":"/menu-img/e62eaaae-6a45-34f3-aa2f-09895deb073c-t.jpg","imgFull":"/menu-img/e62eaaae-6a45-34f3-aa2f-09895deb073c.jpg"},{"name":{"ru":"Cалат со свеклой , апельсином и сыром фета","kk":"Қызылша, апельсин және фета ірімшігі қосылған салат"},"price":"3 600","id":"fa95f6c4-b54f-4a6c-84ca-cc0b2093c650","img":"/menu-img/fa95f6c4-b54f-4a6c-84ca-cc0b2093c650-t.jpg","imgFull":"/menu-img/fa95f6c4-b54f-4a6c-84ca-cc0b2093c650.jpg"},{"name":{"ru":"Салат Баварский с охотничьими колбасками","kk":"Аңшылық шұжықтары бар бавариялық салаты"},"price":"3 600","id":"a77d8e12-0380-4b7d-9232-87c20c390c3d","desc":{"ru":"Салат с куриным филе, охотничьими колбасками помидорами черри, маринованными корнишонами, заправлен горчично майонезным соусом, подается с сыром Чечил","kk":"Тауық еті қосылған салат, аңшылық шұжықтар, шие қызанақтары, маринадталған корнизалар, қыша-майонез соусы қосылған, Чечил ірімшігі қосылған"},"img":"/menu-img/a77d8e12-0380-4b7d-9232-87c20c390c3d-t.jpg","imgFull":"/menu-img/a77d8e12-0380-4b7d-9232-87c20c390c3d.jpg"},{"name":{"ru":"Салат с гречневой лапшой Соба","kk":"Қарақұмық соба кеспесі қосылған салат"},"price":"2 900","id":"1746a6db-0b01-4b3a-a6da-5ebb36936741","desc":{"ru":"В компании овощей и курицы, сдобренная заправкой на основе оливкового масла и соевого соуса, лапша имеет очень приятный вкус и аромат.","kk":"Зәйтүн майы мен соя соусы негізіндегі тұздықпен дәмделген көкөністер мен тауық етінің компаниясында кеспе өте жағымды дәм мен хош иіске ие."},"img":"/menu-img/1746a6db-0b01-4b3a-a6da-5ebb36936741-t.jpg","imgFull":"/menu-img/1746a6db-0b01-4b3a-a6da-5ebb36936741.jpg"},{"name":{"ru":"Теплый салат с утиным филе и сыром Камамбер","kk":"Үйрек филесі мен Камамбер ірімшігі қосылған жылы салат"},"price":"3 700","id":"07195ef0-061f-4cb3-9a58-ff248ab016c5","desc":{"ru":"Микс салатов в сочетании с обжаренным утиным филе, апельсином и сыром Камамбер, заправлен ароматным маслом","kk":"Хош иісті май қосылған қуырылған үйрек филесі, апельсин және Камамбер ірімшігі қосылған салаттар"},"img":"/menu-img/07195ef0-061f-4cb3-9a58-ff248ab016c5-t.jpg","imgFull":"/menu-img/07195ef0-061f-4cb3-9a58-ff248ab016c5.jpg"},{"name":{"ru":"Гриль салат с цыпленком","kk":"Гриль тауықпен салаты"},"price":"3 600","id":"37c41cdf-09a6-4631-be61-f4a0a7d12003","desc":{"ru":"Салат из куриной грудки гриль, с жареными овощами.","kk":"Қуырылған көкөністермен грильдегі тауықтың төс еті салаты."},"img":"/menu-img/37c41cdf-09a6-4631-be61-f4a0a7d12003-t.jpg","imgFull":"/menu-img/37c41cdf-09a6-4631-be61-f4a0a7d12003.jpg"},{"name":{"ru":"Салат с жареной телятиной и баклажанами","kk":"Қуырылған бұзау және баклажан қосылған салат"},"price":"3 900","id":"c5ca910a-f2ef-4d52-872a-47511b42c167","desc":{"ru":"Теплый салат с обжареными баклажанами , телятиной, свежими томатами и сыром Фета","kk":"Қуырылған баклажан, бұзау еті, жаңа піскен қызанақ және фета ірімшігі қосылған жылы салат"},"img":"/menu-img/c5ca910a-f2ef-4d52-872a-47511b42c167-t.jpg","imgFull":"/menu-img/c5ca910a-f2ef-4d52-872a-47511b42c167.jpg"},{"name":{"ru":"Салат с жареной брынзой и овощами","kk":"Қуырылған ірімшігі мен көкөніс қосылған салат"},"price":"3 500","id":"50c5e1c0-4b65-49bf-9053-6af43f4bc219","desc":{"ru":"Листья салата, красный лук, помидоры черри, болгарский перец,обжаренная брынза, Заправлен горчицей и лимонным соком","kk":"Салат, қызыл пияз, шие қызанақтары, болгар бұрышы, қыша және лимон шырыны қосылған қуырылған ірімшігі"},"img":"/menu-img/50c5e1c0-4b65-49bf-9053-6af43f4bc219-t.jpg","imgFull":"/menu-img/50c5e1c0-4b65-49bf-9053-6af43f4bc219.jpg"}],"id":"cb65404e-25d6-3a68-b0a0-ce46c2c6dac5"},{"title":{"ru":"Холодные закуски","kk":"Салқын тіскебасарлар","en":"Cold Appetizers"},"items":[{"name":{"ru":"Рыбное Ассорти","kk":"Балық ассортиі"},"price":"6 900","id":"3be9171c-bc7a-38ee-a44a-7a974f454a32","img":"/menu-img/3be9171c-bc7a-38ee-a44a-7a974f454a32-t.jpg","imgFull":"/menu-img/3be9171c-bc7a-38ee-a44a-7a974f454a32.jpg"},{"name":{"ru":"Ассорти мясное","kk":"Ет ассортиі"},"price":"6 900","id":"1020b5f8-40ae-38d0-aba5-01381c7e5137","img":"/menu-img/1020b5f8-40ae-38d0-aba5-01381c7e5137-t.jpg","imgFull":"/menu-img/1020b5f8-40ae-38d0-aba5-01381c7e5137.jpg"},{"name":{"ru":"Классическое капрезе","kk":"«Ерекше» капрезе"},"price":"3 630","id":"396d87e3-36f1-3204-a347-ad8b374c676a","desc":{"ru":"(Моцарелла гальбанни, свежие томаты и соус песто)","kk":"(баялдымен және песто тұздығымен)"},"img":"/menu-img/396d87e3-36f1-3204-a347-ad8b374c676a-t.jpg","imgFull":"/menu-img/396d87e3-36f1-3204-a347-ad8b374c676a.jpg"},{"name":{"ru":"Винная тарелка","kk":"Ірімшік табақ"},"price":"4 950","id":"40456ac0-31d1-34f1-a9c5-2b9db87ac74b","desc":{"ru":"ассорти сыров, мед, грецкий орех, маслины. Подается без винограда","kk":"ірімшіктер, бал, зәйтүн, грек жаңғағы жүзімсіз беріледі"},"img":"/menu-img/40456ac0-31d1-34f1-a9c5-2b9db87ac74b-t.jpg","imgFull":"/menu-img/40456ac0-31d1-34f1-a9c5-2b9db87ac74b.jpg"},{"name":{"ru":"Ассорти Кавказское","kk":"Кавказ ассортиі"},"price":"3 630","id":"8dff7376-cd41-3ad8-9f11-1f1e3ebf75fb","img":"/menu-img/8dff7376-cd41-3ad8-9f11-1f1e3ebf75fb-t.jpg","imgFull":"/menu-img/8dff7376-cd41-3ad8-9f11-1f1e3ebf75fb.jpg"},{"name":{"ru":"Домашние соленья","kk":"Қолдан дайындалған тұздама"},"price":"2 970","id":"0d624083-8c62-387d-b501-d21fddaf9e36","img":"/menu-img/0d624083-8c62-387d-b501-d21fddaf9e36-t.jpg","imgFull":"/menu-img/0d624083-8c62-387d-b501-d21fddaf9e36.jpg"},{"name":{"ru":"Селедочка под водочку","kk":"Майшабақ жеңіл тағамы"},"price":"2 970","id":"6d7e1c0a-dd0d-328d-8a49-dad2114b7981","img":"/menu-img/6d7e1c0a-dd0d-328d-8a49-dad2114b7981-t.jpg","imgFull":"/menu-img/6d7e1c0a-dd0d-328d-8a49-dad2114b7981.jpg"},{"name":{"ru":"Закуска из баклажан","kk":"Баклажан тағамдары"},"price":"2 900","id":"11ca7ef6-f830-4c8b-91fd-a18c670db0ef","desc":{"ru":"Холодная закуска из баклажанов, заправленных пастой из грецкого ореха, зелени и чеснока","kk":"Жаңғақ пастасы, шөптер және сарымсақ қосылған баклажанның салқын тәбеті"},"img":"/menu-img/11ca7ef6-f830-4c8b-91fd-a18c670db0ef-t.jpg","imgFull":"/menu-img/11ca7ef6-f830-4c8b-91fd-a18c670db0ef.jpg"}],"id":"a2aafb77-2ef6-34c3-a07a-93add91c0e07"},{"title":{"ru":"Закуски","kk":"Тіскебасарлар","en":"Appetizers"},"items":[{"name":{"ru":"Крылышки в панировке «Garage\"","kk":"Фирмалық тауық қанаттары"},"price":"2 530","id":"6daeae88-526c-3438-8f7d-720df87d692d","img":"/menu-img/6daeae88-526c-3438-8f7d-720df87d692d-t.jpg","imgFull":"/menu-img/6daeae88-526c-3438-8f7d-720df87d692d.jpg"},{"name":{"ru":"Куриные наггетсы","kk":"Тауық наггетстері"},"price":"1 550","id":"f4e1c306-9067-3b52-876b-206787389bb9","desc":{"ru":"в соусе Sweet chilli","kk":"Sweet chilli тұздығында"},"img":"/menu-img/f4e1c306-9067-3b52-876b-206787389bb9-t.jpg","imgFull":"/menu-img/f4e1c306-9067-3b52-876b-206787389bb9.jpg"},{"name":{"ru":"Пивные креветки","kk":"Сыраға асшаяндар"},"price":"3 900","id":"b5edb869-884c-3160-8500-065cdb85d570","img":"/menu-img/b5edb869-884c-3160-8500-065cdb85d570-t.jpg","imgFull":"/menu-img/b5edb869-884c-3160-8500-065cdb85d570.jpg"},{"name":{"ru":"Бараньи семечки","kk":"Қойдың қабырғалары"},"price":"3 400","id":"07e3dd2b-072d-34c2-ab9f-9049924a7009","img":"/menu-img/07e3dd2b-072d-34c2-ab9f-9049924a7009-t.jpg","imgFull":"/menu-img/07e3dd2b-072d-34c2-ab9f-9049924a7009.jpg"},{"name":{"ru":"Сырные палочки","kk":"Ірімшік таяқшалары"},"price":"1 800","id":"46c46294-0f5e-3dc3-97c5-d69ef1277a19","img":"/menu-img/46c46294-0f5e-3dc3-97c5-d69ef1277a19-t.jpg","imgFull":"/menu-img/46c46294-0f5e-3dc3-97c5-d69ef1277a19.jpg"},{"name":{"ru":"Жареный сыр чечил","kk":"Қуырылған чечил ірімшігі"},"price":"1 600","id":"caff8a38-6bd6-305f-8a12-fff9d096b519","img":"/menu-img/caff8a38-6bd6-305f-8a12-fff9d096b519-t.jpg","imgFull":"/menu-img/caff8a38-6bd6-305f-8a12-fff9d096b519.jpg"},{"name":{"ru":"Кольца кальмара","kk":"Кальмар сақиналары"},"price":"2 400","id":"b72e0816-7e09-4b59-ae2a-b6bdd8eb6651","img":"/menu-img/b72e0816-7e09-4b59-ae2a-b6bdd8eb6651-t.jpg","imgFull":"/menu-img/b72e0816-7e09-4b59-ae2a-b6bdd8eb6651.jpg"},{"name":{"ru":"Луковые кольца","kk":"Пияз сақиналары"},"price":"1 600","id":"a1c63640-a5dd-4e82-9346-794d90a6e4ad","img":"/menu-img/a1c63640-a5dd-4e82-9346-794d90a6e4ad-t.jpg","imgFull":"/menu-img/a1c63640-a5dd-4e82-9346-794d90a6e4ad.jpg"},{"name":{"ru":"Рыбные стрипсы с картошкой фри","kk":"Француз картоптары бар балық жолақтары"},"price":"3 100","id":"28cc3989-e67a-4c1a-95ed-70bde4e5dca3","img":"/menu-img/28cc3989-e67a-4c1a-95ed-70bde4e5dca3-t.jpg","imgFull":"/menu-img/28cc3989-e67a-4c1a-95ed-70bde4e5dca3.jpg"},{"name":{"ru":"Чубуречки с телятиной и соусом тар тар","kk":"Сиыр еті Чебурек, тар-тар соусымен бірге беріледі"},"price":"1 900","id":"34e72b76-d8b1-4dc3-b1f8-8f3be6781d1c","img":"/menu-img/34e72b76-d8b1-4dc3-b1f8-8f3be6781d1c-t.jpg","imgFull":"/menu-img/34e72b76-d8b1-4dc3-b1f8-8f3be6781d1c.jpg"},{"name":{"ru":"Креветки Темпура","kk":"Асшаяндар Темпура"},"price":"4 600","id":"44170bd4-f83b-4cb8-aa79-112dd7831b5f","img":"/menu-img/44170bd4-f83b-4cb8-aa79-112dd7831b5f-t.jpg","imgFull":"/menu-img/44170bd4-f83b-4cb8-aa79-112dd7831b5f.jpg"},{"name":{"ru":"Ржаные хлебцы с чесноком","kk":"Сарымсақ қосылған қара бидай наны"},"price":"1 200","id":"32d9a2b8-73a2-45d0-9a66-5f0f2f34a208","img":"/menu-img/32d9a2b8-73a2-45d0-9a66-5f0f2f34a208-t.jpg","imgFull":"/menu-img/32d9a2b8-73a2-45d0-9a66-5f0f2f34a208.jpg"},{"name":{"ru":"Чебупелли с семгой","kk":"Лосось қосылған “чебупели”"},"price":"2 500","id":"b2672d84-faf5-42e1-9ea2-6c6c7ea0f2a6","img":"/menu-img/b2672d84-faf5-42e1-9ea2-6c6c7ea0f2a6-t.jpg","imgFull":"/menu-img/b2672d84-faf5-42e1-9ea2-6c6c7ea0f2a6.jpg"}],"id":"dc455461-8ac5-366c-a0c9-6475d25f574f"},{"title":{"ru":"Супы","kk":"Сорпалар","en":"Soups"},"items":[{"name":{"ru":"Лапша по домашнему","kk":"Үй кеспесі"},"price":"1 700","id":"bb9e49b4-d5f4-32c5-817b-87b084a7da0a","img":"/menu-img/bb9e49b4-d5f4-32c5-817b-87b084a7da0a-t.jpg","imgFull":"/menu-img/bb9e49b4-d5f4-32c5-817b-87b084a7da0a.jpg"},{"name":{"ru":"Солянка","kk":"Солянка"},"price":"3 200","id":"2d6a95a7-95f4-3b15-8575-4b40363a1947","img":"/menu-img/2d6a95a7-95f4-3b15-8575-4b40363a1947-t.jpg","imgFull":"/menu-img/2d6a95a7-95f4-3b15-8575-4b40363a1947.jpg"},{"name":{"ru":"Том-ям","kk":"Том-ям"},"price":"3 200","id":"e27274cf-cd5a-3e12-97ac-1987d253e7f7","img":"/menu-img/e27274cf-cd5a-3e12-97ac-1987d253e7f7-t.jpg","imgFull":"/menu-img/e27274cf-cd5a-3e12-97ac-1987d253e7f7.jpg"},{"name":{"ru":"Мини-пельмешки с бульоном","kk":"Шағын тұшпаралар сорпамен"},"price":"2 200","id":"0fc630c5-a950-3e5a-b93f-3482652cdfb3","img":"/menu-img/0fc630c5-a950-3e5a-b93f-3482652cdfb3-t.jpg","imgFull":"/menu-img/0fc630c5-a950-3e5a-b93f-3482652cdfb3.jpg"},{"name":{"ru":"Рамен с курицей","kk":"Тауық қосылған рамен"},"price":"3 100","id":"4814c3e3-dce5-4028-a229-979a6a6c6e16","img":"/menu-img/4814c3e3-dce5-4028-a229-979a6a6c6e16-t.jpg","imgFull":"/menu-img/4814c3e3-dce5-4028-a229-979a6a6c6e16.jpg"},{"name":{"ru":"Рамен с телятиной","kk":"Бұзау еті қосылған рамен"},"price":"3 300","id":"149013a0-3ad9-472b-9166-ee6a27da2792","img":"/menu-img/149013a0-3ad9-472b-9166-ee6a27da2792-t.jpg","imgFull":"/menu-img/149013a0-3ad9-472b-9166-ee6a27da2792.jpg"},{"name":{"ru":"Окрошка","kk":"Окрошка"},"price":"2 200","id":"bde17c08-7900-4ec4-aaa3-ed29f91fadb1","desc":{"ru":"Сезонное блюдо","kk":"Маусымдық тағам"},"img":"/menu-img/bde17c08-7900-4ec4-aaa3-ed29f91fadb1-t.jpg","imgFull":"/menu-img/bde17c08-7900-4ec4-aaa3-ed29f91fadb1.jpg"},{"name":{"ru":"Уха рыбная","kk":"Балық сорпасы"},"price":"2 400","id":"3936cc96-fd4d-41bf-a413-7b2b33a4fe06","desc":{"ru":"Подается с водочкой","kk":"Арақпен бірге беріледі"},"img":"/menu-img/3936cc96-fd4d-41bf-a413-7b2b33a4fe06-t.jpg","imgFull":"/menu-img/3936cc96-fd4d-41bf-a413-7b2b33a4fe06.jpg"}],"id":"815eca6b-3bd1-30aa-bf6d-3f908db57066"},{"title":{"ru":"Паста","kk":"Паста","en":"Pasta"},"items":[{"name":{"ru":"Паста Карбонара","kk":"Карбонара пастасы"},"price":"3 650","id":"21d0f5b8-2685-3679-a78d-24f55bb3c4dc","img":"/menu-img/21d0f5b8-2685-3679-a78d-24f55bb3c4dc-t.jpg","imgFull":"/menu-img/21d0f5b8-2685-3679-a78d-24f55bb3c4dc.jpg"},{"name":{"ru":"Паста с куриным филе","kk":"Тауық еті қосылған паста"},"price":"3 400","id":"47610e8b-dbfa-31df-8401-0372aa1929be","img":"/menu-img/47610e8b-dbfa-31df-8401-0372aa1929be-t.jpg","imgFull":"/menu-img/47610e8b-dbfa-31df-8401-0372aa1929be.jpg"},{"name":{"ru":"Паста с креветками","kk":"Асшяандар қосылған паста"},"price":"3 850","id":"3c2b5865-02d3-399d-b332-9fdb707e4cf4","img":"/menu-img/3c2b5865-02d3-399d-b332-9fdb707e4cf4-t.jpg","imgFull":"/menu-img/3c2b5865-02d3-399d-b332-9fdb707e4cf4.jpg"},{"name":{"ru":"Паста 4 сыра","kk":"Төрт ірімшік паста"},"price":"3 700","id":"699b2e4f-6b79-4134-a5b9-d2fad9b8688d","img":"/menu-img/699b2e4f-6b79-4134-a5b9-d2fad9b8688d-t.jpg","imgFull":"/menu-img/699b2e4f-6b79-4134-a5b9-d2fad9b8688d.jpg"}],"id":"264c1448-0e3f-3ffe-b84f-6dc2c04f2d40"},{"title":{"ru":"Фаст фуд","kk":"Бургерлер","en":"Fast Food"},"items":[{"name":{"ru":"Итальянская булочка Чиабатта с куриным филе и сладкой горчицей","kk":"тауық пен тәтті қыша қосылған сэндвич"},"price":"3 650","id":"4e19a1cf-f52a-4c7b-8379-1067204f5151","desc":{"ru":"Подается с картофелем фри","kk":"Француз картопымен бірге беріледі"},"img":"/menu-img/4e19a1cf-f52a-4c7b-8379-1067204f5151-t.jpg","imgFull":"/menu-img/4e19a1cf-f52a-4c7b-8379-1067204f5151.jpg"},{"name":{"ru":"Итальянская булочка Чиабатта с телятиной","kk":"бұзау сэндвичі"},"price":"3 950","id":"6f3fb0fe-df88-4bd1-88c7-17187185d923","desc":{"ru":"подается с картофелем фри","kk":"француз картопымен бірге беріледі"},"img":"/menu-img/6f3fb0fe-df88-4bd1-88c7-17187185d923-t.jpg","imgFull":"/menu-img/6f3fb0fe-df88-4bd1-88c7-17187185d923.jpg"},{"name":{"ru":"Мини бургер с курицей и салатом Коул слоу","kk":"Тауық пен салат қосылған шағын кішкентай бургер"},"price":"2 900","id":"4cbcf056-d410-4d7a-87d0-aa80c9ec92b8","desc":{"ru":"Булочка-55 грамм, куриное филе, салат айсберг, помидор, салат Коул слоу.","kk":"Тоқаш, тауық еті, айсберг салат, қызанақ, салат Коул слоу"},"img":"/menu-img/4cbcf056-d410-4d7a-87d0-aa80c9ec92b8-t.jpg","imgFull":"/menu-img/4cbcf056-d410-4d7a-87d0-aa80c9ec92b8.jpg"},{"name":{"ru":"Мини- бургер с телятиной и салатом Коул слоу","kk":"Бұзау еті мен салат қосылған кішкентай бургер"},"price":"3 000","id":"a8f7cd1f-2bc6-483d-9878-a6e9f933d908","desc":{"ru":"Булочка-55 гр, говяжья котлета, сыр, салат айсберг, помидор, салат Коул слоу.","kk":"Тоқаш, сиыр еті котлеті, ірімшік, айсберг салаты, қызанақ, салат."},"img":"/menu-img/a8f7cd1f-2bc6-483d-9878-a6e9f933d908-t.jpg","imgFull":"/menu-img/a8f7cd1f-2bc6-483d-9878-a6e9f933d908.jpg"},{"name":{"ru":"Фастфуд сет","kk":"Фаст-фуд жиынтығы"},"price":"9 900","id":"b84ec2ff-aba5-42c4-a623-30fbbdd910f2","desc":{"ru":"Бургер с курицей, бургер с телятиной, итальянская булочка чиабатта с курицей, картофель фри, салат Коул слоу, сырный соус, кетчуп","kk":"Тауық гамбургері, бұзау етінен жасалған гамбургер, итальяндық тауық еті сиабатта тоқаш, фри картоп, салат Коул слоу, ірімшік соусы, кетчуп"},"img":"/menu-img/b84ec2ff-aba5-42c4-a623-30fbbdd910f2-t.jpg","imgFull":"/menu-img/b84ec2ff-aba5-42c4-a623-30fbbdd910f2.jpg"}],"id":"32b7a962-eb76-39ce-b0e0-09a331462e9d"},{"title":{"ru":"Мясо","kk":"Ет","en":"Meat"},"items":[{"name":{"ru":"Rib-Eye стейк","kk":"Rib-Eye стейгі"},"price":"7 990","id":"439d34cd-84e5-3569-b353-477fd735dd8f","desc":{"ru":"(На языке мясников «рибай» обозначает «край на ребре», самый мясистый отруб из передней части туши) подается с сахарной косточкой и хрустящим тостом","kk":"Сиыр етінің ең танымал және әйгілі стейгі."},"img":"/menu-img/439d34cd-84e5-3569-b353-477fd735dd8f-t.jpg","imgFull":"/menu-img/439d34cd-84e5-3569-b353-477fd735dd8f.jpg"},{"name":{"ru":"T-Bone стейк","kk":"T-Bone стейгі"},"price":"7 990","id":"22e2d36b-53b5-360a-b001-831abcd9cc3c","desc":{"ru":"(Ти- Бон также объединяет в себе сразу два вида мяса: самый нежный в мире тендерлойн и яркий, насыщенный стриплойн) подается с сахарной косточкой и хрустящим тостом","kk":"Т-сүйегімен бөлінген, сиыр етінен жасалған Стейк"},"img":"/menu-img/22e2d36b-53b5-360a-b001-831abcd9cc3c-t.jpg","imgFull":"/menu-img/22e2d36b-53b5-360a-b001-831abcd9cc3c.jpg"},{"name":{"ru":"Pepper стейк/Pepper steak","kk":"Pepper стейгі"},"price":"6 600","id":"209dad5b-80f6-38a9-9e55-77fce972418b","desc":{"ru":"(для настоящих ценителей острого мяса.)","kk":"(ащы еттің нағыз білгірлері үшін.)"},"img":"/menu-img/209dad5b-80f6-38a9-9e55-77fce972418b-t.jpg","imgFull":"/menu-img/209dad5b-80f6-38a9-9e55-77fce972418b.jpg"},{"name":{"ru":"Стейк из утиного филе","kk":"Үйрек стейгі"},"price":"4 650","id":"019cf49d-ae5e-4b51-a1ab-c02d409c55c0","desc":{"ru":"Обжаренная до золотистой корочки утиная грудка с зеленой спаржей, помидорами черри и карамелизированной грушей","kk":"Жасыл спаржа, шие қызанақтары және карамельденген алмұрт қосылған алтын қыртысы бар үйрек төсі"},"img":"/menu-img/019cf49d-ae5e-4b51-a1ab-c02d409c55c0-t.jpg","imgFull":"/menu-img/019cf49d-ae5e-4b51-a1ab-c02d409c55c0.jpg"},{"name":{"ru":"Нежнейший стейк из куриного филе","kk":"Тауық стейгі"},"price":"4 300","id":"878bbdfc-326f-403b-a5cf-811cd7e9db97","desc":{"ru":"Нежный Стейк из грудки с круассанами с сыром сулугуни","kk":"Сулугуни ірімшігі круассандары қосылған нәзік төс стейк"},"img":"/menu-img/878bbdfc-326f-403b-a5cf-811cd7e9db97-t.jpg","imgFull":"/menu-img/878bbdfc-326f-403b-a5cf-811cd7e9db97.jpg"},{"name":{"ru":"Стриплойн стейк подается с овощами на гриле","kk":"Стриплоин стейк грильдегі көкөністермен бірге беріледі"},"price":"6 900","id":"0fd7e414-f473-4ec0-bb2f-abec47911f16","img":"/menu-img/0fd7e414-f473-4ec0-bb2f-abec47911f16-t.jpg","imgFull":"/menu-img/0fd7e414-f473-4ec0-bb2f-abec47911f16.jpg"},{"name":{"ru":"Медальоны Бон филе с картофельными кнедликами","kk":"Бон филе медальондары картоп гарнирімен"},"price":"6 700","id":"6be54acb-de8f-41a4-9790-7b7dc27ae3f7","img":"/menu-img/6be54acb-de8f-41a4-9790-7b7dc27ae3f7-t.jpg","imgFull":"/menu-img/6be54acb-de8f-41a4-9790-7b7dc27ae3f7.jpg"},{"name":{"ru":"Бараньи рёбрышки на хоспере с овощами гриль","kk":"Хосперде дайындалған қой қабырғалары гриль көкөністерімен"},"price":"7 200","id":"7b992fbc-257d-40b0-bf4e-7763ff7f128c","desc":{"ru":"Сочные бараньи рёбрышки, приготовленные на хоспере до румяной корочки, с насыщенным ароматом дымка. Подаются с овощами гриль — баклажанами, кабачками, перцем и картофелем — и дополняются пикантным соусом. Сытное и яркое блюдо с настоящим вкусом огня. 🔥🍖","kk":"Хосперде дайындалған, түтіннің қанық хош иісі сіңген, сырты қытырлақ, іші шырынды қой қабырғалары. Грильде қуырылған баклажан, цуккини, бұрыш және картоппен бірге ұсынылады, дәмді соуспен толықтырылған. Оттың нағыз дәмін сездіретін тойымды әрі әсерлі тағам. 🔥🍖"},"img":"/menu-img/7b992fbc-257d-40b0-bf4e-7763ff7f128c-t.jpg","imgFull":"/menu-img/7b992fbc-257d-40b0-bf4e-7763ff7f128c.jpg"}],"id":"08186896-014a-3a00-bb6b-143e9a1c3b5b"},{"title":{"ru":"Рыба","kk":"Балық","en":"Fish"},"items":[{"name":{"ru":"Филе семги в соусе Терияке","kk":"Ақсеркенің сүбе еті Терияке тұздығында"},"price":"6 300","id":"600b0545-e131-3f05-a590-b379a23e44ef","desc":{"ru":"подается с рисом миксом зелени.","kk":"күріш пен аралас көкпен бірге беріледі."},"img":"/menu-img/600b0545-e131-3f05-a590-b379a23e44ef-t.jpg","imgFull":"/menu-img/600b0545-e131-3f05-a590-b379a23e44ef.jpg"},{"name":{"ru":"Судак по-деревенски под сыром пармезан","kk":"Ауыл жағдайындағы көксерке"},"price":"4 700","id":"e687ad57-08b7-3dd8-8dcc-d28a3c593d08","desc":{"ru":"подается с картофелем по домашнему и грибами","kk":"үйдегі картоппен бірге беріледі және саңырауқұлақтар"},"img":"/menu-img/e687ad57-08b7-3dd8-8dcc-d28a3c593d08-t.jpg","imgFull":"/menu-img/e687ad57-08b7-3dd8-8dcc-d28a3c593d08.jpg"}],"id":"b17ce2f3-2e75-31d8-b29c-3772eba6b103"},{"title":{"ru":"Гарниры","kk":"Гарниерлер","en":"Sides"},"items":[{"name":{"ru":"Картофель фри","kk":"Фри картобы"},"price":"990","id":"448f92d9-d3e5-3986-8b0d-10d16d080761","img":"/menu-img/448f92d9-d3e5-3986-8b0d-10d16d080761-t.jpg","imgFull":"/menu-img/448f92d9-d3e5-3986-8b0d-10d16d080761.jpg"},{"name":{"ru":"Картошка по-домашнему с грибами гарнир","kk":"Үй жағдайындағы картоп саңырауқұлақпен"},"price":"1 100","id":"70cb21a4-f623-35db-a362-b7c21fcd6d31","img":"/menu-img/70cb21a4-f623-35db-a362-b7c21fcd6d31-t.jpg","imgFull":"/menu-img/70cb21a4-f623-35db-a362-b7c21fcd6d31.jpg"},{"name":{"ru":"Кукуруза на гриле","kk":"Грильдегі жүгері"},"price":"990","id":"18c501bf-dcbd-3083-beb2-9c18e40109a0","img":"/menu-img/18c501bf-dcbd-3083-beb2-9c18e40109a0-t.jpg","imgFull":"/menu-img/18c501bf-dcbd-3083-beb2-9c18e40109a0.jpg"},{"name":{"ru":"Рис гарнир","kk":"күріші"},"price":"790","id":"5d4a32a6-ec05-3124-9d3a-a8d80083101c","img":"/menu-img/5d4a32a6-ec05-3124-9d3a-a8d80083101c-t.jpg","imgFull":"/menu-img/5d4a32a6-ec05-3124-9d3a-a8d80083101c.jpg"},{"name":{"ru":"Шампиньоны на хоспере","kk":"Хоспердегі қозықұйрық"},"price":"1 650","id":"4f4a214c-233d-3e56-899c-e45fcd489f24"},{"name":{"ru":"Овощи гриль","kk":"Қақталған көкөніс"},"price":"1 900","id":"8d467b2f-db12-33c4-afbc-db6cb721c484","img":"/menu-img/8d467b2f-db12-33c4-afbc-db6cb721c484-t.jpg","imgFull":"/menu-img/8d467b2f-db12-33c4-afbc-db6cb721c484.jpg"},{"name":{"ru":"Капуста квашеная","kk":"Ашытылған қырыққабат"},"price":"550","id":"80977de0-7319-3959-b191-91116512021a","img":"/menu-img/80977de0-7319-3959-b191-91116512021a-t.jpg","imgFull":"/menu-img/80977de0-7319-3959-b191-91116512021a.jpg"},{"name":{"ru":"Картофельные дольки","kk":"Картоп сыналары"},"price":"990","id":"4915ede8-ee5b-43d4-8ad5-7a2d589a67a0"}],"id":"803e9f21-37bc-3814-8ec8-c9f422a91bb0"},{"title":{"ru":"Вторые блюда","kk":"Екінші турлі тамақ","en":"Main Courses"},"items":[{"name":{"ru":"Жареная телятина с картофелем и грибами","kk":"Картоп пен саңырауқұлақ қосылған грильдегі бұзау еті"},"price":"4 290","id":"2d852e42-86f3-4407-9a68-231c40c7d81e","img":"/menu-img/2d852e42-86f3-4407-9a68-231c40c7d81e-t.jpg","imgFull":"/menu-img/2d852e42-86f3-4407-9a68-231c40c7d81e.jpg"},{"name":{"ru":"Колбаски из телятины с квашеной капустой и горчицей","kk":"Қырыққабат пен қыша қосылған бұзау етінен жасалған шұжықтар"},"price":"4 290","id":"572f6feb-eba7-4cbe-a573-d9ea77e630ac","img":"/menu-img/572f6feb-eba7-4cbe-a573-d9ea77e630ac-t.jpg","imgFull":"/menu-img/572f6feb-eba7-4cbe-a573-d9ea77e630ac.jpg"},{"name":{"ru":"Колбаски из баранины с квашеной капустой и горчицей","kk":"Қырыққабат пен қыша қосылған қой етінен жасалған шұжықтар"},"price":"4 290","id":"506f8227-e11b-44c2-97b6-8fc56cd6f5f3","img":"/menu-img/506f8227-e11b-44c2-97b6-8fc56cd6f5f3-t.jpg","imgFull":"/menu-img/506f8227-e11b-44c2-97b6-8fc56cd6f5f3.jpg"},{"name":{"ru":"Колбаски из конины с квашеной капустой и горчицей","kk":"Қырыққабат және қыша қосылған жылқы етінен жасалған шұжықтар"},"price":"4 290","id":"aada8ae5-5ff4-4501-9bbe-e211a6f687d8","img":"/menu-img/aada8ae5-5ff4-4501-9bbe-e211a6f687d8-t.jpg","imgFull":"/menu-img/aada8ae5-5ff4-4501-9bbe-e211a6f687d8.jpg"},{"name":{"ru":"Колбаски из курицы с квашеной капустой и горчицей","kk":"Қырыққабат пен қыша қосылған тауық шұжықтары"},"price":"3 900","id":"2d2b20b9-a0e3-4ad3-832b-85eeec445434","img":"/menu-img/2d2b20b9-a0e3-4ad3-832b-85eeec445434-t.jpg","imgFull":"/menu-img/2d2b20b9-a0e3-4ad3-832b-85eeec445434.jpg"}],"id":"b73c6015-6a25-3b12-b515-21d941f693e6"},{"title":{"ru":"Шашлык","kk":"Кәуаптар","en":"Skewers"},"items":[{"name":{"ru":"Баранина по-кавказски","kk":"қой етінен жасалған кәуап"},"price":"4 900","id":"c3393356-d375-3458-bd55-1be98738e2da","img":"/menu-img/c3393356-d375-3458-bd55-1be98738e2da-t.jpg","imgFull":"/menu-img/c3393356-d375-3458-bd55-1be98738e2da.jpg"},{"name":{"ru":"Шашлык из куриного филе","kk":"Тауық сүбесінен кәуап"},"price":"3 450","id":"fe673f6d-28e8-3fe9-8743-67530f7abea9","img":"/menu-img/fe673f6d-28e8-3fe9-8743-67530f7abea9-t.jpg","imgFull":"/menu-img/fe673f6d-28e8-3fe9-8743-67530f7abea9.jpg"},{"name":{"ru":"Шашлык из куриных крыльев","kk":"Тауық қанаттарынан кәуап"},"price":"3 450","id":"24f0412d-931e-350e-b016-a496bf5e18f9","img":"/menu-img/24f0412d-931e-350e-b016-a496bf5e18f9-t.jpg","imgFull":"/menu-img/24f0412d-931e-350e-b016-a496bf5e18f9.jpg"},{"name":{"ru":"Люля-кебаб","kk":"Люля-кәуап"},"price":"4 100","id":"e5135953-94c3-3eef-b88e-e21489465236","img":"/menu-img/e5135953-94c3-3eef-b88e-e21489465236-t.jpg","imgFull":"/menu-img/e5135953-94c3-3eef-b88e-e21489465236.jpg"},{"name":{"ru":"Шашлык из утиного филе","kk":"Үйрек сүбесінен кәуап"},"price":"3 650","id":"f68d796c-a80f-3ff9-8664-e2a58b5f7846","img":"/menu-img/f68d796c-a80f-3ff9-8664-e2a58b5f7846-t.jpg","imgFull":"/menu-img/f68d796c-a80f-3ff9-8664-e2a58b5f7846.jpg"},{"name":{"ru":"Шашлык из телятины","kk":"Бұзау етінен куәуап"},"price":"4 500","id":"e41feba0-55f4-3658-accd-7598abe5d4e7","img":"/menu-img/e41feba0-55f4-3658-accd-7598abe5d4e7-t.jpg","imgFull":"/menu-img/e41feba0-55f4-3658-accd-7598abe5d4e7.jpg"},{"name":{"ru":"Шашлык свинина мякоть","kk":"Шошқаның жұмсақ етінен кәуап"},"price":"3 950","id":"5f36c63c-3340-358e-8bdc-596760e490c6","img":"/menu-img/5f36c63c-3340-358e-8bdc-596760e490c6-t.jpg","imgFull":"/menu-img/5f36c63c-3340-358e-8bdc-596760e490c6.jpg"},{"name":{"ru":"Шашлык свинина антрекот","kk":"Шошқаның антрекотынан кәуап"},"price":"3 950","id":"a4f8f1a1-798a-37dd-8dfd-46844277016c","img":"/menu-img/a4f8f1a1-798a-37dd-8dfd-46844277016c-t.jpg","imgFull":"/menu-img/a4f8f1a1-798a-37dd-8dfd-46844277016c.jpg"}],"id":"f4282373-cc2c-3c23-bf17-436f688a175e"},{"title":{"ru":"Блюда на компанию","kk":"Компанияға арналған тағамдар","en":"Sharing Platters"},"items":[{"name":{"ru":"Шашлычный микс","kk":"Кәуап миксі"},"price":"29 990","id":"ce050275-21bb-3c7a-85f5-a4d6f607f183","desc":{"ru":"На 7-8 персон. (баранина по кавказски, филе куриное, шашлык из утиного филе, шашлык из телятины, куриные крылышки, овощной шашлык, шампиньоны, соус, лаваш)","kk":"На 7-8 персон. (Кавказ қой еті, тауық еті, кәуап үйрек филесі, пісірілген картоп, тауық қанаттары, лула кебаб, шампиньон кәуабы, соус, лаваш)"},"img":"/menu-img/ce050275-21bb-3c7a-85f5-a4d6f607f183-t.jpg","imgFull":"/menu-img/ce050275-21bb-3c7a-85f5-a4d6f607f183.jpg"},{"name":{"ru":"Ассорти из стейков","kk":"Стейктер ассортиі"},"price":"52 990","id":"80fc4571-7437-37c2-b6c8-6b58b71c8e93","desc":{"ru":"Рекомендуем на 9-10 персон. (T-Вone стейк, Rib- Еуе стейк, Монстр ребро, Pepper стейк, медальоны, стриплойн стейк, стейк из утки, стейк из курицы, подается с овощами гриль, горчицей, соусом BBQ и с соусом сальса)","kk":"Рекомендуем на 9-10 персон. (T-Воne стейк, Rib- Еуе стейк, Монстр қабырғасы, Pepper стейк, картоппен бірге беріледі тілімдері, грильдегі көкөністер, қыша, барбекю соусы және сальса соусымен)"},"img":"/menu-img/80fc4571-7437-37c2-b6c8-6b58b71c8e93-t.jpg","imgFull":"/menu-img/80fc4571-7437-37c2-b6c8-6b58b71c8e93.jpg"},{"name":{"ru":"Ассорти из колбасок по домашнему, с картофельными дольками и квашенной капустой","kk":"Картоп тілімдері мен ашытылған қырыққабат қосылған ассорти үй шұжықтары"},"price":"12 990","id":"e638612f-92aa-4b60-9b7a-ba9bffa31d62","desc":{"ru":"Ассорти колбасок из телятины, конины, курицы и баранины, подается с квашенной капустой, картофельными дольками, сладкой горчицей и соусом BBQ","kk":"Бұзау, жылқы, тауық және қой етінен жасалған шұжықтар, тұздалған қырыққабат, картоп сыналары, тәтті қыша және барбекю соусымен беріледі."},"img":"/menu-img/e638612f-92aa-4b60-9b7a-ba9bffa31d62-t.jpg","imgFull":"/menu-img/e638612f-92aa-4b60-9b7a-ba9bffa31d62.jpg"},{"name":{"ru":"Рыбное ассорти на компанию","kk":"Компанияға арналған балық табақ"},"price":"29 000","id":"cf8f965f-d782-406f-b183-f7ce7f99c5cb","desc":{"ru":"Дорадо на гриле , филе семги, филе судака, кольца кальмара, креветки темпура, стрипсы из судака, продается с рисом , кукурузой, и капустой брокколи","kk":"Күріш, жүгері және брокколимен бірге сатылатын грильдегі дорадо, лосось филесі, көксерке филесі, кальмар сақиналары, темпура асшаяндары, көксерке жолақтары"},"img":"/menu-img/cf8f965f-d782-406f-b183-f7ce7f99c5cb-t.jpg","imgFull":"/menu-img/cf8f965f-d782-406f-b183-f7ce7f99c5cb.jpg"},{"name":{"ru":"Птичий микс на гриле","kk":"Грильдегі құс етіңің қоспасы"},"price":"22 000","id":"fd9ce2b7-09a5-468d-92b3-2959926fde73","desc":{"ru":"Идеально для компании из 6 персон, нежные шашлыки из утки, куриных крыльев и куриного филе, с колбасками на гриле","kk":"6 адамнан тұратын топ үшін қолайлы, Ассорти тауық кебабтары мен тауық қосылған шұжықтар"},"img":"/menu-img/fd9ce2b7-09a5-468d-92b3-2959926fde73-t.jpg","imgFull":"/menu-img/fd9ce2b7-09a5-468d-92b3-2959926fde73.jpg"}],"id":"b7651702-020e-38b9-b399-565ddc570e4a","note":{"ru":"Рекомендуем на 5 персон","kk":"5 адамға ұсыналады"}},{"title":{"ru":"Пивные сеты","kk":"Сыра жиынтығы","en":"Beer Sets"},"items":[{"name":{"ru":"Креветки к пиву для большой компании","kk":"Сыра қоспасы №4"},"price":"15 500","id":"e3b8bb32-c2b2-36df-bd96-62406f5ab859","desc":{"ru":"(килограмм жареных креветок, подается с долькой лимона и средиземноморским соусом)","kk":"(килограмм қуырылған асшаяндар, бір тілім лимонмен бірге беріледі және Жерорта теңізі соусы)"},"img":"/menu-img/e3b8bb32-c2b2-36df-bd96-62406f5ab859-t.jpg","imgFull":"/menu-img/e3b8bb32-c2b2-36df-bd96-62406f5ab859.jpg"},{"name":{"ru":"Сет к пиву (рыбный)","kk":"Сыра жинағы (балық)"},"price":"12 900","id":"1ba27acc-e119-443f-b788-ff88c67af9bc","desc":{"ru":"Кольца кальмара, луковые кольца, креветки, чипсы из лаваша, креветки теипура, рыбные стрипсы, креветки жареные, лимон","kk":"Кальмар сақиналары, пияз сақиналары, асшаяндар, лаваш чипсы, тейпура асшаяндары, балық жолақтары, қуырылған асшаяндар, лимон"},"img":"/menu-img/1ba27acc-e119-443f-b788-ff88c67af9bc-t.jpg","imgFull":"/menu-img/1ba27acc-e119-443f-b788-ff88c67af9bc.jpg"},{"name":{"ru":"Квиз сет","kk":"Квиз жинағы"},"price":"9 990","id":"4aaff673-c9f8-49ac-9809-ed1eaba70cc5","desc":{"ru":"Сырные палочки, крылышки BBQ , чебуреки, куриные стрипсы, 2 бургера, соус сырный, кетчуп","kk":"Ірімшік таяқшалары, барбекю қанаттары, чебуреки, тауық еті, 2 бургер, ірімшік соусы, кетчуп"},"img":"/menu-img/4aaff673-c9f8-49ac-9809-ed1eaba70cc5-t.jpg","imgFull":"/menu-img/4aaff673-c9f8-49ac-9809-ed1eaba70cc5.jpg"},{"name":{"ru":"Пивная тарелка 1","kk":"Сыра табақ 1"},"price":"9 900","id":"a649a5b0-d5ee-4965-a6e4-b9d7f7b4eca2","desc":{"ru":"Жареный чечил, гарлики , жареные пельмешки, охотничьи колбаски, луковые кольца, бараньи семечки, 3 вида соуса","kk":"Қуырылған чечил, сарымсақ, қуырылған тұшпара, аңшы шұжығы, пияз сақинасы, қой етінің тұқымы, тұздықтың 3 түрі"},"img":"/menu-img/a649a5b0-d5ee-4965-a6e4-b9d7f7b4eca2-t.jpg","imgFull":"/menu-img/a649a5b0-d5ee-4965-a6e4-b9d7f7b4eca2.jpg"}],"id":"a8541a2a-a577-3002-b947-d25ab649b531"},{"title":{"ru":"Соусы","kk":"Тұздықтар","en":"Sauces"},"items":[{"name":{"ru":"Грибной соус","kk":"Саңырауқұлақ соусы"},"price":"350","id":"465d5b3f-083f-311d-a1c4-4bc5b4b64338"},{"name":{"ru":"Тар-тар соус","kk":"Тар-тар соусы"},"price":"350","id":"bafdcb41-e746-3b01-9dab-5c5e57fe17c2"},{"name":{"ru":"BBQ соус","kk":"BBQ соусы"},"price":"350","id":"222f7e6e-d1d6-366f-9e4d-427e3884e65e"},{"name":{"ru":"Луизиана соус","kk":"Луизиана соусы"},"price":"350","id":"8b0a25a0-1f2e-329a-9057-cfbb44ef4266"},{"name":{"ru":"Кетчуп","kk":"Кетчуп"},"price":"250","id":"28a3ed06-b2d2-353b-adfc-46277aca196b"},{"name":{"ru":"Демиглас соус","kk":"Демиглас соусы"},"price":"350","id":"10d9533b-f40f-3def-a5f5-e2343d208dd5"},{"name":{"ru":"Сальса соус","kk":"Сальса соусы"},"price":"350","id":"1653a424-cc3f-36ac-87d7-4e9d60db800f"},{"name":{"ru":"Майонез","kk":"Майонез"},"price":"250","id":"664cbdab-e111-3600-858d-8f0b3a190408"},{"name":{"ru":"Сметанно-чесночный соус","kk":"Қаймақ және сарымсақ соусы"},"price":"250","id":"65afe2bb-6721-34d9-9e1d-d753454fe942"},{"name":{"ru":"Сырный соус","kk":"ірімшік соусы"},"price":"350","id":"3fa8be12-d734-4ae5-be07-8b0a49430ea2"}],"id":"b7d039eb-7cd4-39e4-878c-f7b499fed4b9"},{"title":{"ru":"Мучные изделия","kk":"ұн өнімдері","en":"Breads & Pastry"},"items":[{"name":{"ru":"Хлебная корзина","kk":"Нан себеті"},"price":"880","id":"fde109d3-448a-3b80-88d8-2ddc4b56abb3"},{"name":{"ru":"Лаваш","kk":"Лаваш"},"price":"300","id":"3ade0d59-56af-3478-91eb-9692da891417","img":"/menu-img/3ade0d59-56af-3478-91eb-9692da891417-t.jpg","imgFull":"/menu-img/3ade0d59-56af-3478-91eb-9692da891417.jpg"}],"id":"594bd4b1-3e42-396d-9a09-cce76bc1d561"},{"title":{"ru":"Десерты","kk":"Десерттер","en":"Desserts"},"items":[{"name":{"ru":"Мороженое 1 шарик","kk":"Балмұздақ (1 қасық)"},"price":"990","id":"d7602a77-2570-32ff-8978-b32da8fc9a60"},{"name":{"ru":"Штрудель яблочный","kk":"алма қосылған штрудель"},"price":"2 100","id":"ff5c2e80-3826-3730-90d2-cf5bd687daf9","img":"/menu-img/ff5c2e80-3826-3730-90d2-cf5bd687daf9-t.jpg","imgFull":"/menu-img/ff5c2e80-3826-3730-90d2-cf5bd687daf9.jpg"},{"name":{"ru":"Штрудель вишневый","kk":"Шие қосылған штрудель"},"price":"2 100","id":"504929cd-b19a-3209-98d2-a801c24d9c65","img":"/menu-img/504929cd-b19a-3209-98d2-a801c24d9c65-t.jpg","imgFull":"/menu-img/504929cd-b19a-3209-98d2-a801c24d9c65.jpg"}],"id":"68197217-86da-3835-b92f-7391fd5002c9"}],"bar":[{"title":{"ru":"Акции","kk":"Жеңілдіктер","en":"Specials"},"note":{"ru":"Наличие акций уточняйте у официанта","kk":"Акциялардың бар-жоғын даяшыдан сұраңыз","en":"Ask your waiter which specials are available today"},"items":[{"name":{"ru":"Сэт фирменных настоек 1+1","kk":"Фирмалық тұнбалар сеті 1+1"},"price":"7 100","id":"aae445d8-a0b0-4333-8fbe-7accef767898","desc":{"ru":"обычная цена 14 200","kk":"бұрынғы бағасы 14 200"},"img":"/menu-img/aae445d8-a0b0-4333-8fbe-7accef767898-t.jpg","imgFull":"/menu-img/aae445d8-a0b0-4333-8fbe-7accef767898.jpg"},{"name":{"ru":"Водка Хортиця Ice 0,5 + Coca Cola 1l","kk":"Хортиця Ice арағы 0,5 л + Coca Cola 1 л"},"price":"11 000","id":"8117837a-64ac-4245-8a35-6e317e0c70d5","desc":{"ru":"обычная цена 13 000","kk":"бұрынғы бағасы 13 000"},"img":"/menu-img/8117837a-64ac-4245-8a35-6e317e0c70d5-t.jpg","imgFull":"/menu-img/8117837a-64ac-4245-8a35-6e317e0c70d5.jpg"},{"name":{"ru":"William Lawson`s spised 0.7 L+ Coca cola 1 L"},"price":"21 990","id":"0e365afa-aa56-4613-85b6-2eb00a4654bd","img":"/menu-img/0e365afa-aa56-4613-85b6-2eb00a4654bd-t.jpg","imgFull":"/menu-img/0e365afa-aa56-4613-85b6-2eb00a4654bd.jpg"}],"id":"promo-bar"},{"title":{"ru":"Пиво","kk":"Сыра","en":"Draft Beer"},"items":[{"name":{"ru":"Garage светлое","kk":"Garage ашық сырасы"},"price":"990","id":"81c7bc56-30ee-3279-9d55-4c02648bac29","price2":"1 600","desc":{"ru":"330 мл / 500 мл. Фирменный светлый лагер — золотистый, освежающий и лёгкий, с мягким солодовым вкусом и приятной хмелевой горчинкой.","kk":"330 мл / 500 мл. Фирмалық ашық лагер: алтын түсті, сергітетін әрі жеңіл, жұмсақ уыт дәмі мен жағымды құлмақ ащылығы бар."},"img":"/menu-img/81c7bc56-30ee-3279-9d55-4c02648bac29-t.jpg","imgFull":"/menu-img/81c7bc56-30ee-3279-9d55-4c02648bac29.jpg"},{"name":{"ru":"Garage тёмное","kk":"Garage қара сырасы"},"price":"990","id":"5524f95f-686f-3af8-a73b-7c0888453edc","price2":"1 600","desc":{"ru":"330 мл / 500 мл. Насыщенное тёмное пиво с нотами карамели и обжаренного солода, мягкой горчинкой и плотной кремовой пеной.","kk":"330 мл / 500 мл. Карамель мен қуырылған уыт реңктері, жұмсақ ащылығы және қою кілегейлі көбігі бар бай дәмді қара сыра."},"img":"/menu-img/5524f95f-686f-3af8-a73b-7c0888453edc-t.jpg","imgFull":"/menu-img/5524f95f-686f-3af8-a73b-7c0888453edc.jpg"},{"name":{"ru":"Carlsberg (Дания)","kk":"Carlsberg (Дания)"},"price":"1 600","id":"116ab239-415e-3e6c-bd50-2fb78de72f82","price2":"2 300","desc":{"ru":"0,33 мл / 500 мл. Carlsberg классический (датский пилснер) — светлое пиво с освежающим чистым вкусом. Аромат гармоничный, с солодовыми травянисто-хмелевыми нотками.","kk":"0,33 мл / 500 мл. Классикалық Carlsberg (даниялық пилснер): сергітетін таза дәмі бар ашық сыра. Иісі үйлесімді, уыт пен шөп-құлмақ реңктері бар."},"img":"/menu-img/116ab239-415e-3e6c-bd50-2fb78de72f82-t.jpg","imgFull":"/menu-img/116ab239-415e-3e6c-bd50-2fb78de72f82.jpg"},{"name":{"ru":"Paulaner 0,5 ml (Германия)","kk":"Paulaner 0,5 мл (Германия)"},"price":"3 300","id":"fb5f1534-78f4-4483-a1bf-9b680e997e4f","desc":{"ru":"500 мл. Пиво светлое фильтрованное пастеризованное. Вкус: Мягкий и гармоничный, с преобладанием солодовых ноток, легкой сладостью и тонкими оттенками фруктов, меда или пряностей.","kk":"500 мл. Жеңіл сүзілген пастерленген сыра"},"img":"/menu-img/fb5f1534-78f4-4483-a1bf-9b680e997e4f-t.jpg","imgFull":"/menu-img/fb5f1534-78f4-4483-a1bf-9b680e997e4f.jpg"},{"name":{"ru":"Kronenburg 1664 (Франция)","kk":"Kronenburg 1664 (Франция)"},"price":"1 700","id":"2a05e8fb-0d30-363d-9f8b-102be8f7aba4","price2":"2 500","desc":{"ru":"330 мл / 500 мл. французское светлое нефильтрованное пшеничное пиво класса супер-премиум с мягким фруктово-цитрусовым вкусом.","kk":"330 мл / 500 мл. Жұмсақ жеміс-цитрус дәмі бар француздық супер-премиум сыныпты ашық сүзілмеген бидай сырасы."},"img":"/menu-img/2a05e8fb-0d30-363d-9f8b-102be8f7aba4-t.jpg","imgFull":"/menu-img/2a05e8fb-0d30-363d-9f8b-102be8f7aba4.jpg"},{"name":{"ru":"Grimbergen Double Ambree (Бельгия)","kk":"Grimbergen Double Ambree"},"price":"2 190","id":"ea70ad7b-108f-38ad-972a-605851f1b724","price2":"3 390","desc":{"ru":"330 мл / 500 мл. Тёмное фильтрованное пиво со сладко-горьким вкусом. В аромате слышны яркие ноты карамели, шоколада, изюма и сухофруктов. Крепкое пиво лучше всего сочетается с сытными мясными блюдами благодаря богатому вкусу, в котором чувствуется аромат обжаренного солода.","kk":"330 мл / 500 мл. Тәтті-ащы дәмі бар қара сүзілген сыра. Иісінде карамель, шоколад, мейіз және кептірілген жемістердің жарқын реңктері сезіледі. Қуырылған уыт иісі бар бай дәмінің арқасында бұл күшті сыра тойымды ет тағамдарымен жақсы үйлеседі."},"img":"/menu-img/ea70ad7b-108f-38ad-972a-605851f1b724-t.jpg","imgFull":"/menu-img/ea70ad7b-108f-38ad-972a-605851f1b724.jpg"}],"id":"e5f9045f-14b4-3df9-b09d-72177bdcfb8a"},{"title":{"ru":"Пиво в бутылке","kk":"Бөтелкедегі сыра","en":"Bottled Beer"},"items":[{"name":{"ru":"Carlsberg (безалкогольное, светлое) 0,5 ml","kk":"Carlsberg (алкогольсіз, ашық) 0,5 мл"},"price":"1 990","id":"451a8ebb-57bb-3e1e-b1d7-ebfa83c5d817","desc":{"ru":"450 мл. Вкус: Чистый и освежающий, с выраженными тонами светлого солода, легкой фруктовой кислинкой и мягкой хмелевой горчинкой в сухом послевкусии.","kk":"450 мл. Дәмі: таза әрі сергітетін, ашық уыттың айқын реңктері, жеңіл жеміс қышқылдығы және құрғақ соңғы дәмде жұмсақ құлмақ ащылығы бар."},"img":"/menu-img/451a8ebb-57bb-3e1e-b1d7-ebfa83c5d817-t.jpg","imgFull":"/menu-img/451a8ebb-57bb-3e1e-b1d7-ebfa83c5d817.jpg"},{"name":{"ru":"Carlsberg 0,5 ml"},"price":"2 200","id":"d5a99f40-b97c-3d35-b595-520a016c47f3","desc":{"ru":"Классический датский лагер Вкус: Чистый и освежающий, с выраженными тонами светлого солода, легкой фруктовой кислинкой и мягкой хмелевой горчинкой в сухом послевкусии.","kk":"Классикалық даниялық лагер. Дәмі: таза әрі сергітетін, ашық уыттың айқын реңктері, жеңіл жеміс қышқылдығы және құрғақ соңғы дәмде жұмсақ құлмақ ащылығы бар."},"img":"/menu-img/d5a99f40-b97c-3d35-b595-520a016c47f3-t.jpg","imgFull":"/menu-img/d5a99f40-b97c-3d35-b595-520a016c47f3.jpg"},{"name":{"ru":"Пиво Heineken N/A 0,33 ml","kk":"Сыра Heineken N/A 0,33 ml"},"price":"2 500","id":"6eb98c34-1f04-4313-85bd-027ce39571f3","desc":{"ru":"330 мл. Вкус пива освежающий, с нотками солода, хлеба, трав и длительным послевкусием с тонкими травяными нотками хмеля.","kk":"330 мл. Сыраның дәмі сергітетін, уыт, нан, шөп реңктері бар, соңғы дәмінде құлмақтың нәзік шөп реңктері ұзақ сезіледі."},"img":"/menu-img/6eb98c34-1f04-4313-85bd-027ce39571f3-t.jpg","imgFull":"/menu-img/6eb98c34-1f04-4313-85bd-027ce39571f3.jpg"},{"name":{"ru":"Пиво HEINEKEN 0,33 ml","kk":"HEINEKEN сырасы 0,33 мл"},"price":"2 500","id":"10709efd-79ff-427b-938c-bfa116d97fdd","desc":{"ru":"При его создании используются микс различных сортов хмеля и эксклюзивные дрожжи Heineken-A, благодаря которым напиток приобретает тонкий аромат с тонами фруктов. Во вкусе выделяются оттенки жареных каштанов, банана и карамели на фоне сбалансированной горьковатой ноты.","kk":"Әртүрлі құлмақ сорттары мен Heineken-A эксклюзивті ашытқысының арқасында сусынның жемістің нәзік иісі бар. Дәмінде қуырылған талшын, банан және карамель реңктері теңгерімді ащы нотамен үйлеседі."},"img":"/menu-img/10709efd-79ff-427b-938c-bfa116d97fdd-t.jpg","imgFull":"/menu-img/10709efd-79ff-427b-938c-bfa116d97fdd.jpg"},{"name":{"ru":"Guinnes Draught 0,44 ml"},"price":"4 200","id":"57a1911b-c69a-328b-b179-e6193a095457","desc":{"ru":"Легендарный ирландский сухой стаут с уникальной азотной капсулой внутри. Напиток обладает глубоким темным цветом, нотками жженого солода, кофе, шоколада и легкой дымной горчинкой при сбалансированной сладости.","kk":"Ішінде ерекше азот капсуласы бар аңызға айналған ирланд құрғақ стауты. Түсі қою, күйген уыт, кофе, шоколад реңктері және теңгерімді тәттілікпен үйлескен жеңіл түтінді ащылығы бар."},"img":"/menu-img/57a1911b-c69a-328b-b179-e6193a095457-t.jpg","imgFull":"/menu-img/57a1911b-c69a-328b-b179-e6193a095457.jpg"},{"name":{"ru":"Somersby Blackberry 0,43 ml","kk":"Somersby Blackberry 0,43 ml"},"price":"1 890","id":"ae31bf28-e30b-4e21-9358-fe9d043b6255","desc":{"ru":"Сидр со вкусом ежевики, один из самых популярных вкусов в линейке Somersby. Обладает натуральным мягким вкусом, приятной сладостью и легкой кислинкой в послевкусии.","kk":"Somersby желісіндегі ең танымал дәмдердің бірі: қара бүлдірген дәмді сидр. Табиғи жұмсақ дәмі, жағымды тәттілігі және соңғы дәмінде жеңіл қышқылдығы бар."},"img":"/menu-img/ae31bf28-e30b-4e21-9358-fe9d043b6255-t.jpg","imgFull":"/menu-img/ae31bf28-e30b-4e21-9358-fe9d043b6255.jpg"},{"name":{"ru":"Paulaner 0,5 ml"},"price":"3 300","id":"12e0f8e7-e2f7-480b-b890-b23218a549ea","desc":{"ru":"классический баварский светлый лагер (хеллес) крепостью около 4,9%, производимый знаменитой мюнхенской пивоварней.","kk":"Атақты Мюнхен сыра зауыты шығаратын классикалық бавариялық ашық лагер (хеллес), күштілігі шамамен 4,9%."},"img":"/menu-img/12e0f8e7-e2f7-480b-b890-b23218a549ea-t.jpg","imgFull":"/menu-img/12e0f8e7-e2f7-480b-b890-b23218a549ea.jpg"},{"name":{"ru":"Holsten Light 0,5 ml"},"price":"2 200","id":"49412450-0953-4378-a6e8-b68de212bc8f","desc":{"ru":"Мягкий солодовенный вкус и едва заметная хмелевая горчинка в сочетании с легким цветочным ароматом позволяют всецело насладиться пивом с немецким качеством. Крепость 4,0%.","kk":"Жұмсақ уыт дәмі, білінер-білінбес құлмақ ащылығы және жеңіл гүл иісі неміс сапасындағы сырадан толық ләззат алуға мүмкіндік береді. Күштілігі 4,0%."},"img":"/menu-img/49412450-0953-4378-a6e8-b68de212bc8f-t.jpg","imgFull":"/menu-img/49412450-0953-4378-a6e8-b68de212bc8f.jpg"}],"id":"d746312d-d36c-3ab6-98f8-b56b368147fe"},{"title":{"ru":"К пиву","kk":"Снэктар","en":"Beer Snacks"},"items":[{"name":{"ru":"Фисташки","kk":"Пісте"},"price":"2 300","id":"4f1c9ad1-b3ba-3cda-933c-fec0d9e7b58c","desc":{"ru":"Высококачественные фисташки – традиционная закуска к пиву, сытный перекус. 100 гр","kk":"Жоғары сапалы пісте: сыраға дәстүрлі тіскебасар, тойымды жеңіл тамақ. 100 г"},"img":"/menu-img/4f1c9ad1-b3ba-3cda-933c-fec0d9e7b58c-t.jpg","imgFull":"/menu-img/4f1c9ad1-b3ba-3cda-933c-fec0d9e7b58c.jpg"},{"name":{"ru":"Сыр чечил","kk":"Шешіл ірімшігі"},"price":"1 400","id":"1aae37f1-0eac-32fb-a7a4-b04826b4a69b","desc":{"ru":"Традиционный армянский волокнистый рассольный сыр, идеальная закуска к пенному. 100 гр.","kk":"Дәстүрлі армян талшықты тұздық ірімшігі, сыраға таптырмас тіскебасар. 100 г."},"img":"/menu-img/1aae37f1-0eac-32fb-a7a4-b04826b4a69b-t.jpg","imgFull":"/menu-img/1aae37f1-0eac-32fb-a7a4-b04826b4a69b.jpg"},{"name":{"ru":"Соленый арахис","kk":"Тұздалған жержаңғақ"},"price":"1 300","id":"10ed77eb-9376-3885-8e53-5c90481cbf16","desc":{"ru":"Популярная соленая закуска к пиву, которая ценится за насыщенный вкус и питательность. 100 гр.","kk":"Бай дәмі мен құнарлылығы үшін бағаланатын сыраға арналған танымал тұзды тіскебасар. 100 г."},"img":"/menu-img/10ed77eb-9376-3885-8e53-5c90481cbf16-t.jpg","imgFull":"/menu-img/10ed77eb-9376-3885-8e53-5c90481cbf16.jpg"}],"id":"ab7dbcff-6006-32bf-bce3-cf642cb43789"},{"title":{"ru":"Водка","kk":"Арақ","en":"Vodka"},"items":[{"name":{"ru":"Водка “Elyx” 0,05 ml","kk":"“Elyx” арағы"},"price":"1 990","id":"368603ae-ca6e-4e91-90e6-39c26dea7d65","desc":{"ru":"это суперпремиальная водка класса люкс, известная своей исключительной мягкостью, которую называют \"жидким шелком\". Производится вручную из озимой пшеницы с одного шведского поместья. Отличается богатым вкусом с нотками хлеба, белого шоколада и специй.","kk":"Бұл супер премиум класты сәнді арақ өзінің ерекше жұмсақтығымен танымал, оны «сұйық жібек» деп атайды. 1921 жылдан бері сақталған ежелгі мыс ыдыстарда тазартылған, бір швед жерінен алынған күздік бидайдан қолдан жасалған, нан, ақ шоколад және дәмдеуіштердің бай дәмімен мақтана алады."},"img":"/menu-img/368603ae-ca6e-4e91-90e6-39c26dea7d65-t.jpg","imgFull":"/menu-img/368603ae-ca6e-4e91-90e6-39c26dea7d65.jpg"},{"name":{"ru":"Водка Absolut Оригинальная классическая 0,05 ml","kk":"Absolut түпнұсқа классикалық арақ"},"price":"1 900","id":"17400471-83f5-4b29-b1a0-7836a4426a65","desc":{"ru":"Absolut Blue — классическая шведская водка, известная своим кристально чистым вкусом и мягкой текстурой.","kk":"Absolut Blue: кристалдай таза дәмі мен жұмсақ құрылымымен танымал классикалық швед арағы."},"img":"/menu-img/17400471-83f5-4b29-b1a0-7836a4426a65-t.jpg","imgFull":"/menu-img/17400471-83f5-4b29-b1a0-7836a4426a65.jpg"},{"name":{"ru":"Водка Grey GOOSE 0,05 ml","kk":"Grey GOOSE арағы 0,05 мл"},"price":"2 290","id":"ac3dd1b8-3ab1-315e-9604-6c76ce02e6a5","desc":{"ru":"50 мл. Изысканная водка “Grey Goose” по праву считается самой вкусной водкой в мире, являясь лидером среди крепких напитков в классе ультра-премиум","kk":"50 мл. Ультра-премиум сыныптағы күшті сусындар көшбасшысы, нәзік «Grey Goose» арағы әлемдегі ең дәмді арақ саналады"},"img":"/menu-img/ac3dd1b8-3ab1-315e-9604-6c76ce02e6a5-t.jpg","imgFull":"/menu-img/ac3dd1b8-3ab1-315e-9604-6c76ce02e6a5.jpg"},{"name":{"ru":"Водка KYZYLZHAR Legend of Kazakhstan 0,05 ml","kk":"KYZYLZHAR Legend of Kazakhstan арағы 0,05 мл"},"price":"1 350","id":"b0a45f5b-28a4-3077-a25b-f3d8aa425126","desc":{"ru":"50 мл. классическая казахстанская водка крепостью 40% Вкус: Мягкий, чистый и сбалансированный, с тонкими зерновыми оттенками и гладким послевкусием без резкости.","kk":"50 мл. Күштілігі 40% классикалық қазақстандық арақ. Дәмі: жұмсақ, таза әрі теңгерімді, нәзік дән реңктері және өткірлігі жоқ біркелкі соңғы дәмі бар."},"img":"/menu-img/b0a45f5b-28a4-3077-a25b-f3d8aa425126-t.jpg","imgFull":"/menu-img/b0a45f5b-28a4-3077-a25b-f3d8aa425126.jpg"},{"name":{"ru":"Водка QAZAQ ELI Legend of Qazaqstan 0,05 ml","kk":"QAZAQ ELI Legend of Qazaqstan арағы 0,05 мл"},"price":"1 450","id":"5de7eea0-d89c-41ef-93ed-58607686e01b","desc":{"ru":"50 мл. Очистка уникального купажа из высококачественного солодового спирта \"Альфа\" серебряной фильтрацией и углем черной березы придает водке фирменный благородный вкус настоящей легенды Казахстана.","kk":"50 мл. «Альфа» жоғары сапалы уыт спиртінен жасалған бірегей купажды күміс сүзгімен және қара қайың көмірімен тазарту арағына нағыз Қазақстан аңызына лайық асыл дәм береді."},"img":"/menu-img/5de7eea0-d89c-41ef-93ed-58607686e01b-t.jpg","imgFull":"/menu-img/5de7eea0-d89c-41ef-93ed-58607686e01b.jpg"},{"name":{"ru":"Водка Хортиця Ice 0.05 мл","kk":"Хортиця Ice Арағы"},"price":"1 200","id":"6e31de3a-cba0-4a44-8531-8b3bd756ccb7","desc":{"ru":"Особая водка в линейке бренда «Хортиця», главной фишкой которой является изменение цвета бутылки при охлаждении.","kk":"Хортыця бренд желісіндегі арнайы арақ, оның басты ерекшелігі - салқындаған кезде бөтелкенің түсін өзгертуі."},"img":"/menu-img/6e31de3a-cba0-4a44-8531-8b3bd756ccb7-t.jpg","imgFull":"/menu-img/6e31de3a-cba0-4a44-8531-8b3bd756ccb7.jpg"},{"name":{"ru":"Водка ALPHA 0,05 ml","kk":"ALPHA арағы 0,05 мл"},"price":"1 200","id":"b7c13eba-aab0-47f6-800f-963819dd11de","desc":{"ru":"0,05 мл. Производится на основе высококачественного спирта класса «Альфа», который изготавливается исключительно из зерна (пшеницы или ржи). Этот спирт ценится за минимальное содержание примесей, что обеспечивает продукту мягкий вкус.","kk":"0,05 мл. Alpha арағы тек дәннен (бидай немесе қара бидай) жасалған жоғары сапалы Альфа сұрыпты спиртті пайдаланып өндіріледі. Бұл спирт өнімге жұмсақ дәм беретін минималды қоспа мөлшерімен бағаланады."},"img":"/menu-img/b7c13eba-aab0-47f6-800f-963819dd11de-t.jpg","imgFull":"/menu-img/b7c13eba-aab0-47f6-800f-963819dd11de.jpg"}],"id":"ade8f9e1-6790-3f8d-97b6-efb6f4d73fa2"},{"title":{"ru":"Настойки фирменные","kk":"Брендтік тұнбалар","en":"House Infusions"},"items":[{"name":{"ru":"Настойка Медовуха 0,05 ml","kk":"Бал тұнбасы (медовуха) 0,05 мл"},"price":"950","id":"d619d132-b088-3fb7-a4d3-beff07642995","desc":{"ru":"50 мл. Медовая настойка (медовуха) — это крепкий алкогольный напиток с мягким медовым вкусом и согревающим эффектом, который готовится путем настаивания.","kk":"50 мл. Бал тұнбасы (медовуха): тұндыру арқылы дайындалатын, жұмсақ бал дәмі мен жылытатын әсері бар күшті сусын."},"img":"/menu-img/d619d132-b088-3fb7-a4d3-beff07642995-t.jpg","imgFull":"/menu-img/d619d132-b088-3fb7-a4d3-beff07642995.jpg"},{"name":{"ru":"Настойка на вишне 0,05 ml","kk":"Шие тұнбасы 0,05 мл"},"price":"950","id":"7e5cf73d-2321-3174-ba03-a34dfe50853c","desc":{"ru":"50 мл. Вишнёвая настойка на водке (вишнёвка) — это популярный домашний алкогольный напиток с насыщенным рубиновым цветом, мягким ягодным вкусом и легким миндальным ароматом.","kk":"50 мл. Арақтағы шие тұнбасы (вишнёвка): қою лағыл түсті, жұмсақ жидек дәмі мен жеңіл бадам иісі бар танымал үй сусыны."},"img":"/menu-img/7e5cf73d-2321-3174-ba03-a34dfe50853c-t.jpg","imgFull":"/menu-img/7e5cf73d-2321-3174-ba03-a34dfe50853c.jpg"},{"name":{"ru":"Настойка на клюкве 0,05 ml","kk":"Мүкжидек тұнбасы 0,05 мл"},"price":"950","id":"ea38bb37-a78e-3e7e-bec7-84a30d2e58fa","desc":{"ru":"50 мл. Клюквенная настойка на водке (или «клюковка») — это популярный домашний алкогольный напиток на натуральных ягодах, приятного рубинового цвета с мягким кисло-сладким вкусом, который легко пьется и маскирует спиртовую резкость.","kk":"50 мл. Арақтағы мүкжидек тұнбасы («клюковка»): табиғи жидектерден жасалған, жағымды лағыл түсті, жұмсақ қышқыл-тәтті дәмі бар, оңай ішілетін әрі спирттің өткірлігін басатын танымал үй сусыны."},"img":"/menu-img/ea38bb37-a78e-3e7e-bec7-84a30d2e58fa-t.jpg","imgFull":"/menu-img/ea38bb37-a78e-3e7e-bec7-84a30d2e58fa.jpg"},{"name":{"ru":"Настойка на облепихе 0,05 ml","kk":"Шырғанақ тұнбасы 0,05 мл"},"price":"950","id":"e75d3a83-030a-392a-a746-a9e00d532a33","desc":{"ru":"50 мл. Облепиховая настойка на водке — это яркий, ароматный и полезный домашний напиток с красивым янтарным цветом и насыщенным кисло-сладким вкусом.","kk":"50 мл. Арақтағы шырғанақ тұнбасы: әдемі кәріптас түсті, бай қышқыл-тәтті дәмі бар жарқын, хош иісті әрі пайдалы үй сусыны."},"img":"/menu-img/e75d3a83-030a-392a-a746-a9e00d532a33-t.jpg","imgFull":"/menu-img/e75d3a83-030a-392a-a746-a9e00d532a33.jpg"},{"name":{"ru":"Настойка на смородине 0,05 ml","kk":"Қарақат тұнбасы 0,05 мл"},"price":"950","id":"a33babb7-31d4-35d4-b5de-1abc54a798eb","desc":{"ru":"50 мл. Домашняя настойка на смородине на водке — это ароматный и мягкий алкогольный напиток с насыщенным ягодным вкусом и красивым рубиновым цветом.","kk":"50 мл. Арақтағы үй қарақат тұнбасы: бай жидек дәмі мен әдемі лағыл түсі бар хош иісті әрі жұмсақ сусын."},"img":"/menu-img/a33babb7-31d4-35d4-b5de-1abc54a798eb-t.jpg","imgFull":"/menu-img/a33babb7-31d4-35d4-b5de-1abc54a798eb.jpg"},{"name":{"ru":"Настойка лимончелло 0,05 ml","kk":"Лимончелло тұнбасы 0,05 мл"},"price":"950","id":"ecc3aa34-9349-3cb2-9487-bbed55c42165","desc":{"ru":"50 мл. Лимончелло на водке — это домашний аналог знаменитого итальянского лимонного ликёра, который отличается мягким цитрусовым вкусом, умеренной сладостью и меньшей крепостью по сравнению с оригиналом на чистом спирту.","kk":"50 мл. Арақтағы лимончелло: әйгілі итальяндық лимон ликерінің үй нұсқасы, жұмсақ цитрус дәмі, орташа тәттілігі бар және таза спирттегі түпнұсқадан әлсіздеу."},"img":"/menu-img/ecc3aa34-9349-3cb2-9487-bbed55c42165-t.jpg","imgFull":"/menu-img/ecc3aa34-9349-3cb2-9487-bbed55c42165.jpg"},{"name":{"ru":"Настойка из базилика 0,05 ml","kk":"Райхан тұнбасы 0,05 мл"},"price":"950","id":"b954826b-1967-4360-be59-cc9257e6db14","desc":{"ru":"50 мл. Ароматный алкогольный напиток домашнего приготовления на основе водки, спирта или самогона с ярким травянистым вкусом и красивым изумрудным оттенком.","kk":"50 мл. Арақ, спирт немесе самогон негізінде үйде жасалатын хош иісті сусын: шөпті жарқын дәмі мен әдемі зүмірет реңкі бар."},"img":"/menu-img/b954826b-1967-4360-be59-cc9257e6db14-t.jpg","imgFull":"/menu-img/b954826b-1967-4360-be59-cc9257e6db14.jpg"}],"id":"9b7f41aa-219e-30c3-8842-cd474c357271"},{"title":{"ru":"Виски Ирландия","kk":"Ирланд вискиі","en":"Irish Whiskey"},"items":[{"name":{"ru":"JAMESON ORIGINAL 0,05 ml"},"price":"2 750","id":"f99132ce-a7b2-3c93-ad23-33bb12685b83","desc":{"ru":"50 мл. Классический флагманский бленд с универсальным мягким вкусом. Вкус: Исключительно мягкий, сбалансированный, с тонами спелых фруктов, ванили, легкой сладостью хереса и ореховым послевкусием.","kk":"50 мл. Әмбебап жұмсақ дәмі бар классикалық флагмандық бленд. Дәмі: өте жұмсақ, теңгерімді, піскен жемістер, ваниль, херестің жеңіл тәттілігі және жаңғақты соңғы дәмі бар."},"img":"/menu-img/f99132ce-a7b2-3c93-ad23-33bb12685b83-t.jpg","imgFull":"/menu-img/f99132ce-a7b2-3c93-ad23-33bb12685b83.jpg"},{"name":{"ru":"JAMESON CRESTED 0,05 ml"},"price":"3 300","id":"bb0a023b-2c28-3fe4-a344-8ced5d1d64df","desc":{"ru":"50 мл. Мягкий ирландский купажированный виски крепостью 40%, в котором преобладает доля зернового виски и виски категории pot still Вкус: Мягкий и гладкий, сочетает тонкие нюансы хереса, спелых фруктов, пряностей, а также поджаренной древесины и шоколада.","kk":"50 мл. Дән вискиі мен pot still вискиінің үлесі басым жұмсақ ирланд купаждалған вискиі, күштілігі 40%. Дәмі: жұмсақ әрі біркелкі, херестің, піскен жемістердің, дәмдеуіштердің, қуырылған ағаш пен шоколадтың нәзік реңктерін үйлестіреді."},"img":"/menu-img/bb0a023b-2c28-3fe4-a344-8ced5d1d64df-t.jpg","imgFull":"/menu-img/bb0a023b-2c28-3fe4-a344-8ced5d1d64df.jpg"},{"name":{"ru":"Jameson Black Barrel 0,05 ml","kk":"Jameson Black Barrel"},"price":"3 590","id":"ba5d5135-488a-4599-a79c-5031839fca34","desc":{"ru":"0,05 мл. это премиальный ирландский купажированный виски крепостью 40%, созданный с использованием уникальной технологии двойного обжига бочек (double charred).","kk":"0,05 мл. Бөшкелерді екі рет күйдіру (double charred) бірегей технологиясымен жасалған премиум ирланд купаждалған вискиі, күштілігі 40%."},"img":"/menu-img/ba5d5135-488a-4599-a79c-5031839fca34-t.jpg","imgFull":"/menu-img/ba5d5135-488a-4599-a79c-5031839fca34.jpg"}],"id":"0d7becfb-899c-3c7a-8703-c7097116c0db","note":{"ru":"Виски Kilbeggan","kk":"Kilbeggan вискиі"}},{"title":{"ru":"Виски Шотландия","kk":"Шотланд вискиі","en":"Scotch Whisky"},"items":[{"name":{"ru":"Chivas 12 Y.O. 0,05 ml"},"price":"4 299","id":"c150744a-c347-3689-934a-8df8b02830e4","desc":{"ru":"50 мл. Знаменитый шведский/шотландский купажированный виски крепостью 40%, созданный из отборных зерновых и солодовых спиртов с минимальной выдержкой 12 лет.","kk":"50 мл. Кемінде 12 жыл ұсталған таңдаулы дән және уыт спирттерінен жасалған әйгілі шотланд купаждалған вискиі, күштілігі 40%."},"img":"/menu-img/c150744a-c347-3689-934a-8df8b02830e4-t.jpg","imgFull":"/menu-img/c150744a-c347-3689-934a-8df8b02830e4.jpg"},{"name":{"ru":"DEWAR`S Caribbean 8 Y.O. 0,05 ml"},"price":"2 300","id":"8bd45b46-6862-36d6-8bfe-50d42dd2c79f","desc":{"ru":"50 мл. Шотландский купажированный виски с финишной выдержкой в бочках из-под карибского рома.","kk":"50 мл. Кариб ромынан босаған бөшкелерде соңғы ұсталымнан өткен шотланд купаждалған вискиі."},"img":"/menu-img/8bd45b46-6862-36d6-8bfe-50d42dd2c79f-t.jpg","imgFull":"/menu-img/8bd45b46-6862-36d6-8bfe-50d42dd2c79f.jpg"},{"name":{"ru":"Ballantine’s 7 YO 0,05 ml"},"price":"2 350","id":"e428c8d7-c132-438a-899f-7f8cd2c22d67","desc":{"ru":"0,05 мл. Купажированный шотландский виски крепостью 40%, который сочетает классический шотландский стиль и сладкие ноты американского бурбона.","kk":"0,05 мл. Классикалық шотланд стилі мен американдық бурбонның тәтті реңктерін үйлестіретін шотланд купаждалған вискиі, күштілігі 40%."},"img":"/menu-img/e428c8d7-c132-438a-899f-7f8cd2c22d67-t.jpg","imgFull":"/menu-img/e428c8d7-c132-438a-899f-7f8cd2c22d67.jpg"},{"name":{"ru":"Ballantine’s Finest 0,05 ml"},"price":"1 990","id":"eb5532dc-53a8-4988-b883-0338ecff3539","desc":{"ru":"0,05 мл. Легендарный шотландский купажированный виски с мягким сливочным вкусом и светло-золотистым цветом, созданный по рецепту 1910 года.","kk":"0,05 мл. 1910 жылғы рецепт бойынша жасалған жұмсақ кілегейлі дәмі мен ашық алтын түсі бар аңызға айналған шотланд купаждалған вискиі."},"img":"/menu-img/eb5532dc-53a8-4988-b883-0338ecff3539-t.jpg","imgFull":"/menu-img/eb5532dc-53a8-4988-b883-0338ecff3539.jpg"},{"name":{"ru":"WILLIAM LAWSON`S 0,05 ml"},"price":"1 790","id":"480c335b-1f68-38cb-8432-53c4e8c5a9b3","desc":{"ru":"50 мл. Популярный купажированный шотландский виски (скотч) крепостью 40%.","kk":"50 мл. Танымал купаждалған шотланд вискиі (скотч), күштілігі 40%."},"img":"/menu-img/480c335b-1f68-38cb-8432-53c4e8c5a9b3-t.jpg","imgFull":"/menu-img/480c335b-1f68-38cb-8432-53c4e8c5a9b3.jpg"},{"name":{"ru":"WILLIAM LAWSON`S Super Spiced 0,05 ml"},"price":"1 790","id":"6b5d1014-e627-38a7-add9-6876168ee43e","desc":{"ru":"50 мл. Крепкий спиртной напиток на основе купажированного виски крепостью 35%, настоянный на пряностях и натуральных добавках.","kk":"50 мл. Дәмдеуіштер мен табиғи қоспаларға тұндырылған купаждалған виски негізіндегі күшті сусын, күштілігі 35%."},"img":"/menu-img/6b5d1014-e627-38a7-add9-6876168ee43e-t.jpg","imgFull":"/menu-img/6b5d1014-e627-38a7-add9-6876168ee43e.jpg"}],"id":"165183cf-69e3-3665-8282-c843f5458fbf"},{"title":{"ru":"Ром “Havana”","kk":"“Havana” ромы","en":"Havana Club Rum"},"items":[{"name":{"ru":"Rum HAVANA CLUB 3 YO 0,05 ml"},"price":"1 650","id":"cd4c526b-2a18-438c-a963-aca4c14ba8c7","desc":{"ru":"0,05 мл. Популярный кубинский светлый ром крепостью 40%, который создают из патоки сахарного тростника и выдерживают в дубовых бочках около трех лет.","kk":"0,05 мл. Қант қамысының сірнесінен жасалып, емен бөшкелерде шамамен үш жыл ұсталатын танымал кубалық ашық ром, күштілігі 40%."},"img":"/menu-img/cd4c526b-2a18-438c-a963-aca4c14ba8c7-t.jpg","imgFull":"/menu-img/cd4c526b-2a18-438c-a963-aca4c14ba8c7.jpg"},{"name":{"ru":"Rum HAVANA CLUB 7YO 0,05 ml"},"price":"2 300","id":"72729f58-b1b1-409e-a7e7-26dd2cec29f2","desc":{"ru":"0,05 мл. Легендарный кубинский темно-красный ром крепостью 40%, обладающий сложным и богатым вкусом.","kk":"0,05 мл. Күрделі әрі бай дәмі бар аңызға айналған кубалық қою қызыл ром, күштілігі 40%."},"img":"/menu-img/72729f58-b1b1-409e-a7e7-26dd2cec29f2-t.jpg","imgFull":"/menu-img/72729f58-b1b1-409e-a7e7-26dd2cec29f2.jpg"},{"name":{"ru":"Rum HAVANA CLUB Cuban spiced 0,05 ml"},"price":"1 900","id":"c4a7bc24-f140-408a-9ee4-7775faae8372","desc":{"ru":"0,05ml. Пряный кубинский напиток крепостью 35%, в котором классические ромовые спирты соединены с ароматами тропических фруктов и пряностей.","kk":"0,05ml. Классикалық ром спирттері тропикалық жемістер мен дәмдеуіштердің иісімен үйлескен дәмдеуішті кубалық сусын, күштілігі 35%."},"img":"/menu-img/c4a7bc24-f140-408a-9ee4-7775faae8372-t.jpg","imgFull":"/menu-img/c4a7bc24-f140-408a-9ee4-7775faae8372.jpg"},{"name":{"ru":"Rum LAMB’S spiced 0,05 ml"},"price":"1 600","id":"29000aea-d2af-43f3-baad-84dce79ae462","desc":{"ru":"0,05 мл. Мягкий пряный ромовый напиток крепостью 30%, созданный на основе купажа карибских ромов из Гаяны, Ямайки, Барбадоса и Тринидада.","kk":"0,05 мл. Гайана, Ямайка, Барбадос және Тринидад ромдарының купажы негізінде жасалған жұмсақ дәмдеуішті ром сусыны, күштілігі 30%."},"img":"/menu-img/29000aea-d2af-43f3-baad-84dce79ae462-t.jpg","imgFull":"/menu-img/29000aea-d2af-43f3-baad-84dce79ae462.jpg"}],"id":"1f22b8c3-1d95-40e1-b28d-1479c85021b8"},{"title":{"ru":"Ром Bacardi","kk":"Bacardi Ром","en":"Bacardi Rum"},"items":[{"name":{"ru":"OAKHEART 0,05 ml"},"price":"1 890","id":"f84829cf-7e2c-3ef6-9c65-62ff25f66246","desc":{"ru":"50 мл. популярный крепкий напиток (часто классифицируемый как пряный ром) крепостью 35%, созданный знаменитым брендом Bacardi. Название переводится как «дубовое сердце».","kk":"50 мл. Атақты Bacardi бренді жасаған танымал күшті сусын (көбіне дәмдеуішті ром деп аталады), күштілігі 35%. Атауы «емен жүрек» деп аударылады."},"img":"/menu-img/f84829cf-7e2c-3ef6-9c65-62ff25f66246-t.jpg","imgFull":"/menu-img/f84829cf-7e2c-3ef6-9c65-62ff25f66246.jpg"},{"name":{"ru":"BACARDI Carta Negra 0,05 ml"},"price":"1 890","id":"ae7a0766-ea06-3c1a-8d2d-c10c0843459f","desc":{"ru":"50 мл. Насыщенный темный ром крепостью 40%, обладающий мягким и глубоким вкусом.","kk":"50 мл. Жұмсақ әрі терең дәмі бар бай қара ром, күштілігі 40%."},"img":"/menu-img/ae7a0766-ea06-3c1a-8d2d-c10c0843459f-t.jpg","imgFull":"/menu-img/ae7a0766-ea06-3c1a-8d2d-c10c0843459f.jpg"},{"name":{"ru":"BACARDI Carta Blanca 0,05 ml"},"price":"1 890","id":"4a1adc02-ebdf-3523-aafe-2f1641a98372","desc":{"ru":"50 мл. Легендарный светлый (белый) ром крепостью 40%, созданный как универсальная основа для коктейлей. (А значит кока кола станет с ним значительно вкусней ;)","kk":"50 мл. Коктейльдерге әмбебап негіз ретінде жасалған аңызға айналған ашық (ақ) ром, күштілігі 40%. (Демек, кока-кола онымен әлдеқайда дәмді болады ;)"},"img":"/menu-img/4a1adc02-ebdf-3523-aafe-2f1641a98372-t.jpg","imgFull":"/menu-img/4a1adc02-ebdf-3523-aafe-2f1641a98372.jpg"}],"id":"b401c3dd-646f-3a90-b92f-4410091644fc"},{"title":{"ru":"Джин","kk":"Джин","en":"Gin"},"items":[{"name":{"ru":"Malfy Originali Jin 0,05 ml"},"price":"2 400","id":"5859405f-25d3-494d-a7ca-f30c5846e23b","desc":{"ru":"0,05 мл. Премиальный сухой итальянский джин крепостью 41%, произведенный в регионе Пьемонт с использованием чистейшей горной воды из источника Монвизо.","kk":"0,05 мл. Пьемонт өңірінде Монвизо бұлағының таза тау суымен жасалған премиум құрғақ итальяндық джин, күштілігі 41%."},"img":"/menu-img/5859405f-25d3-494d-a7ca-f30c5846e23b-t.jpg","imgFull":"/menu-img/5859405f-25d3-494d-a7ca-f30c5846e23b.jpg"},{"name":{"ru":"Beefeater London Dry Gin 0,05 ml"},"price":"1 950","id":"7211f452-7173-3e54-a700-26be51d3c046","desc":{"ru":"50 мл. Легендарный классический лондонский сухой джин крепостью 40%, обладающий кристально чистым цветом и сбалансированным вкусом с доминирующими нотами можжевельника.","kk":"50 мл. Кристалдай мөлдір түсі және арша иісі басым теңгерімді дәмі бар аңызға айналған классикалық Лондон құрғақ джині, күштілігі 40%."},"img":"/menu-img/7211f452-7173-3e54-a700-26be51d3c046-t.jpg","imgFull":"/menu-img/7211f452-7173-3e54-a700-26be51d3c046.jpg"},{"name":{"ru":"Beefeater Pink Strawberry Gin 0,05 ml"},"price":"1 950","id":"10bdb8a0-b1fe-3f10-a507-a4710b5ba8ed","desc":{"ru":"50 мл. Розовый клубничный джин крепостью 37,5%, созданный на основе классического лондонского сухого джина с добавлением натурального экстракта спелой клубники.","kk":"50 мл. Классикалық Лондон құрғақ джині негізінде піскен құлпынайдың табиғи сығындысын қосып жасалған қызғылт құлпынай джині, күштілігі 37,5%."},"img":"/menu-img/10bdb8a0-b1fe-3f10-a507-a4710b5ba8ed-t.jpg","imgFull":"/menu-img/10bdb8a0-b1fe-3f10-a507-a4710b5ba8ed.jpg"}],"id":"b9a6c7b2-c3c8-3647-89f8-9254895791a8"},{"title":{"ru":"Бренди","kk":"Бренди","en":"Brandy"},"items":[{"name":{"ru":"Martell VS 0,05 ml"},"price":"4 200","id":"47cfadb3-893f-4b64-8de5-c6315945d1f7","desc":{"ru":"0,05 мл. Молодой французский коньяк (выдержка спиртов от 2 лет) крепостью 40%, выпускаемый одним из старейших домов Martell.","kk":"0,05 мл. Ең көне Martell үйінің жас француз коньягы (спирттер кемінде 2 жыл ұсталған), күштілігі 40%."},"img":"/menu-img/47cfadb3-893f-4b64-8de5-c6315945d1f7-t.jpg","imgFull":"/menu-img/47cfadb3-893f-4b64-8de5-c6315945d1f7.jpg"},{"name":{"ru":"Ararat Akhtamar 10 YO 0,05 ml"},"price":"3 300","id":"1206edf7-71de-4f0b-920e-10afa2882cca","desc":{"ru":"0,05ml. Премиальный армянский коньяк (категории КВВК — коньяк выдержанный высшего качества) крепостью 40%, выпускаемый Ереванским коньячным заводом.","kk":"0,05ml. Ереван коньяк зауыты шығаратын премиум армян коньягы (КВВК санаты: жоғары сапалы ұзақ ұсталған коньяк), күштілігі 40%."},"img":"/menu-img/1206edf7-71de-4f0b-920e-10afa2882cca-t.jpg","imgFull":"/menu-img/1206edf7-71de-4f0b-920e-10afa2882cca.jpg"},{"name":{"ru":"Ararat Apricot 0,05 ml"},"price":"2 300","id":"77633064-2b66-4e22-87ee-7de5d0d707bf","desc":{"ru":"0,05 мл. Спиртной напиток крепостью 35%, созданный Ереванским коньячным заводом на основе шестилетнего армянского коньяка с добавлением натурального экстракта спелого абрикоса.","kk":"0,05 мл. Ереван коньяк зауыты алты жылдық армян коньягы негізінде піскен өріктің табиғи сығындысын қосып жасаған сусын, күштілігі 35%."},"img":"/menu-img/77633064-2b66-4e22-87ee-7de5d0d707bf-t.jpg","imgFull":"/menu-img/77633064-2b66-4e22-87ee-7de5d0d707bf.jpg"},{"name":{"ru":"Арарат 5* 0,05 ml","kk":"Арарат 5* 0,05 мл"},"price":"1 890","id":"23a0cace-23d5-3395-8571-c5fabd589888","desc":{"ru":"50 мл. Популярный армянский пятилетний напиток производства Ереванского коньячного завода.","kk":"50 мл. Ереван коньяк зауыты шығаратын танымал бес жылдық армян сусыны."},"img":"/menu-img/23a0cace-23d5-3395-8571-c5fabd589888-t.jpg","imgFull":"/menu-img/23a0cace-23d5-3395-8571-c5fabd589888.jpg"}],"id":"351b5493-6936-424d-a251-e37f8a350e54"},{"title":{"ru":"Коньяк","kk":"Коньяк","en":"Cognac"},"items":[{"name":{"ru":"Казахстан Бахус 5* 0,05 ml","kk":"«Казахстан» Бахус 5* 0,05 мл"},"price":"1 650","id":"56676db6-c71f-34e9-b8f9-01dac5051157","desc":{"ru":"50 мл. Коньяк «Казахстан» 5 звёзд от казахстанского производителя АО «Бахус» — это классический пятилетний ординарный коньяк крепостью 40%.","kk":"50 мл. Қазақстандық «Бахус» АҚ өндірген «Казахстан» 5 жұлдыз коньягы: күштілігі 40% классикалық бес жылдық қатардағы коньяк."},"img":"/menu-img/56676db6-c71f-34e9-b8f9-01dac5051157-t.jpg","imgFull":"/menu-img/56676db6-c71f-34e9-b8f9-01dac5051157.jpg"},{"name":{"ru":"Казахстан Бахус 3* 0,05 ml","kk":"«Казахстан» Бахус 3* 0,05 мл"},"price":"1 450","id":"d0b4c0c8-3445-3ca1-9fa5-90e86754e02f","desc":{"ru":"50 мл. Трехлетний коньяк «Казахстан 3 звезды» от казахстанского производителя Bacchus (АО «Бахус») изготавливается из коньячных дистиллятов с выдержкой не менее трех лет.","kk":"50 мл. Қазақстандық Bacchus («Бахус» АҚ) өндірген «Казахстан 3 жұлдыз» үш жылдық коньягы кемінде үш жыл ұсталған коньяк дистилляттарынан жасалады."},"img":"/menu-img/d0b4c0c8-3445-3ca1-9fa5-90e86754e02f-t.jpg","imgFull":"/menu-img/d0b4c0c8-3445-3ca1-9fa5-90e86754e02f.jpg"}],"id":"20785d75-78e6-30de-9c35-f85d7f7300f1"},{"title":{"ru":"Текила Premium","kk":"Текила премиум","en":"Premium Tequila"},"items":[{"name":{"ru":"Olmeca Altos Playa 100% Agave 0,05 ml"},"price":"2 450","id":"2b0fe4f3-33b3-45b2-90f2-ea9bb0792c41","desc":{"ru":"0,05 мл. Премиальная мексиканская текила категории Blanco (серебряная), изготовленная из 100% голубой агавы сорта Blue Weber. Напиток производят в высокогорном регионе Лос-Альтос (штат Халиско, город Арандас) на высоте 2100 метров над уровнем моря.","kk":"0,05 мл. 100% көгілдір Blue Weber агавасынан жасалған Blanco (күміс) санатындағы премиум мексикалық текила. Сусын теңіз деңгейінен 2100 метр биіктікте, Лос-Альтос таулы өңірінде (Халиско штаты, Арандас қаласы) өндіріледі."},"img":"/menu-img/2b0fe4f3-33b3-45b2-90f2-ea9bb0792c41-t.jpg","imgFull":"/menu-img/2b0fe4f3-33b3-45b2-90f2-ea9bb0792c41.jpg"},{"name":{"ru":"Olmeca Silver 0,05 ml"},"price":"1 750","id":"cafcb84b-7475-4b11-8aeb-6f90d9b2d2e9","desc":{"ru":"0,05 мл. Классическая мексиканская текила класса Mixto (с содержанием сока голубой агавы не менее 51%, остальное — дистилляты из сахарного тростника) крепостью 35%.","kk":"0,05 мл. Mixto сыныбындағы классикалық мексикалық текила (көгілдір агава шырыны кемінде 51%, қалғаны қант қамысының дистилляттары), күштілігі 35%."},"img":"/menu-img/cafcb84b-7475-4b11-8aeb-6f90d9b2d2e9-t.jpg","imgFull":"/menu-img/cafcb84b-7475-4b11-8aeb-6f90d9b2d2e9.jpg"},{"name":{"ru":"Olmeca gold 0,05 ml"},"price":"2 000","id":"cb7affe2-78a5-4ad5-953e-a4a3e49baddf","desc":{"ru":"0,05 мл. Популярная «золотая» текила класса mixto, которая производится из сока голубой агавы (не менее 51%) с добавлением других спиртов и карамели.","kk":"0,05 мл. Көгілдір агава шырынынан (кемінде 51%) басқа спирттер мен карамель қосып жасалатын mixto сыныбындағы танымал «алтын» текила."},"img":"/menu-img/cb7affe2-78a5-4ad5-953e-a4a3e49baddf-t.jpg","imgFull":"/menu-img/cb7affe2-78a5-4ad5-953e-a4a3e49baddf.jpg"}],"id":"8aac4dc7-cfb3-3852-b427-3cf275aee297"},{"title":{"ru":"Вермуты / Биттеры / Аперитивы","kk":"Вермуттар / Ащылар / Аперитивтер","en":"Vermouths / Bitters / Aperitifs"},"items":[{"name":{"ru":"Aperol 0,05 ml"},"price":"1 750","id":"308b0061-a679-3f53-b2ad-2e90a5a43fb4","desc":{"ru":"50 мл. Знаменитый итальянский горько-сладкий аперитив ярко-оранжевого цвета крепостью 11%.","kk":"50 мл. Ашық қызғылт сары түсті әйгілі итальяндық ащы-тәтті аперитив, күштілігі 11%."},"img":"/menu-img/308b0061-a679-3f53-b2ad-2e90a5a43fb4-t.jpg","imgFull":"/menu-img/308b0061-a679-3f53-b2ad-2e90a5a43fb4.jpg"},{"name":{"ru":"Sambuca 0,05 ml"},"price":"1 750","id":"e0a4ce0c-83f8-35c9-bcd7-cc62ac6b47ad","desc":{"ru":"50 мл. Классический итальянский анисовый ликёр крепостью 38%, который выпускается семейной компанией Girolamo Luxardo.","kk":"50 мл. Girolamo Luxardo отбасылық компаниясы шығаратын классикалық итальяндық анис ликері, күштілігі 38%."},"img":"/menu-img/e0a4ce0c-83f8-35c9-bcd7-cc62ac6b47ad-t.jpg","imgFull":"/menu-img/e0a4ce0c-83f8-35c9-bcd7-cc62ac6b47ad.jpg"},{"name":{"ru":"Baileys original irish cream 0,05 ml"},"price":"1 750","id":"ccc8a386-fb20-3346-8df8-d3cc77714bc5","desc":{"ru":"50 мл. Знаменитый на весь мир ирландский сливочный ликёр крепостью 17%, обладающий бархатистой текстурой и нежным сладким вкусом.","kk":"50 мл. Барқыттай құрылымы мен нәзік тәтті дәмі бар әлемге әйгілі ирланд кілегей ликері, күштілігі 17%."},"img":"/menu-img/ccc8a386-fb20-3346-8df8-d3cc77714bc5-t.jpg","imgFull":"/menu-img/ccc8a386-fb20-3346-8df8-d3cc77714bc5.jpg"},{"name":{"ru":"Kahlua 0,05 ml"},"price":"1 750","id":"e4d04f41-fa47-3447-884b-6eda9c9790cd","desc":{"ru":"50 мл. Знаменитый мексиканский кофейный ликёр тёмно-коричневого цвета с насыщенным вкусом черного кофе, карамели и ванили.","kk":"50 мл. Қара кофе, карамель және ванильдің бай дәмі бар қою қоңыр түсті әйгілі мексикалық кофе ликері."},"img":"/menu-img/e4d04f41-fa47-3447-884b-6eda9c9790cd-t.jpg","imgFull":"/menu-img/e4d04f41-fa47-3447-884b-6eda9c9790cd.jpg"},{"name":{"ru":"Absinthe Luxardo 0,05 ml"},"price":"1 850","id":"0966afba-f636-3c50-ae99-1d061a8ef27b","desc":{"ru":"50 мл. Крепкий алкогольный напиток (70%) насыщенного изумрудно-зеленого цвета, созданный известным итальянским производителем по классическому рецепту полынной настойки.","kk":"50 мл. Атақты итальяндық өндіруші жусан тұнбасының классикалық рецептімен жасаған қою зүмірет түсті күшті сусын (70%)."},"img":"/menu-img/0966afba-f636-3c50-ae99-1d061a8ef27b-t.jpg","imgFull":"/menu-img/0966afba-f636-3c50-ae99-1d061a8ef27b.jpg"},{"name":{"ru":"Cointreau 0,05 ml"},"price":"1 750","id":"7875468c-9002-37ab-9c93-759bfcf6446c","desc":{"ru":"50 мл. Знаменитый французский прозрачный апельсиновый ликёр премиум-класса категории трипл-сек (triple sec) крепостью 40%.","kk":"50 мл. Triple sec санатындағы премиум сыныпты әйгілі француздық мөлдір апельсин ликері, күштілігі 40%."},"img":"/menu-img/7875468c-9002-37ab-9c93-759bfcf6446c-t.jpg","imgFull":"/menu-img/7875468c-9002-37ab-9c93-759bfcf6446c.jpg"},{"name":{"ru":"MARTINI Fiero 0,10 ml"},"price":"2 300","id":"fad8fa1a-7f60-31d7-8a1b-baffc72e1053","desc":{"ru":"100 мл. Современный красный апельсиновый вермут крепостью 15%, созданный итальянской компанией Martini & Rossi.","kk":"100 мл. Итальяндық Martini & Rossi компаниясы жасаған заманауи қызыл апельсин вермуты, күштілігі 15%."},"img":"/menu-img/fad8fa1a-7f60-31d7-8a1b-baffc72e1053-t.jpg","imgFull":"/menu-img/fad8fa1a-7f60-31d7-8a1b-baffc72e1053.jpg"},{"name":{"ru":"MARTINI Bianco 0,10 ml"},"price":"2 100","id":"0dd6bc05-399e-3b32-9254-05174cd1f87c","desc":{"ru":"100 мл. Знаменитый итальянский белый сладкий вермут крепостью 15%, созданный на основе белого вина с добавлением пряных трав и ванили.","kk":"100 мл. Ақ шарап негізінде, хош иісті шөптер мен ваниль қосып жасалған әйгілі итальяндық ақ тәтті вермут, күштілігі 15%."},"img":"/menu-img/0dd6bc05-399e-3b32-9254-05174cd1f87c-t.jpg","imgFull":"/menu-img/0dd6bc05-399e-3b32-9254-05174cd1f87c.jpg"},{"name":{"ru":"MARTINI Extra Dry 0,10 ml"},"price":"2 100","id":"6348d565-186f-3562-a981-af870bfdf287","desc":{"ru":"100 мл. Легендарный итальянский белый сухой вермут с минимальным количеством сахара и повышенной крепостью.","kk":"100 мл. Қант мөлшері ең аз әрі күштірек аңызға айналған итальяндық ақ құрғақ вермут."},"img":"/menu-img/6348d565-186f-3562-a981-af870bfdf287-t.jpg","imgFull":"/menu-img/6348d565-186f-3562-a981-af870bfdf287.jpg"},{"name":{"ru":"MARTINI Rosso 0,10 ml"},"price":"2 100","id":"08ea2cc5-1101-3671-bb07-d13f6e7f45d1","desc":{"ru":"100 мл. Классический итальянский красный сладкий вермут с насыщенным пряным вкусом и темным янтарно-красным оттенком.","kk":"100 мл. Классикалық итальяндық қызыл тәтті вермут: дәмі бай әрі хош иісті, түсі қою кәріптас-қызыл."},"img":"/menu-img/08ea2cc5-1101-3671-bb07-d13f6e7f45d1-t.jpg","imgFull":"/menu-img/08ea2cc5-1101-3671-bb07-d13f6e7f45d1.jpg"},{"name":{"ru":"MARTINI RISERVA Bitter 0,10 ml"},"price":"2 100","id":"1ffcce1b-965f-3a18-bd9c-5e245b9182b9","desc":{"ru":"100 мл. Премиальный итальянский аперитив из Турина с насыщенным рубиново-малиновым цветом и крепостью 28,5%.","kk":"100 мл. Туриннің премиум итальяндық аперитиві: түсі қою лағыл-таңқурай, күштілігі 28,5%."},"img":"/menu-img/1ffcce1b-965f-3a18-bd9c-5e245b9182b9-t.jpg","imgFull":"/menu-img/1ffcce1b-965f-3a18-bd9c-5e245b9182b9.jpg"}],"id":"014c601e-b060-327b-b9ff-c7a9a5e6d709"},{"title":{"ru":"Ice Cold Shot -18 °С","kk":"Мұздай шот -18 °С","en":"Ice Cold Shot -18 °C"},"items":[{"name":{"ru":"Jägermeister 0,05ml"},"price":"2 200","id":"df80f9fd-9d32-483b-ba07-42ceb1e9e0d2","desc":{"ru":"Знаменитый немецкий крепкий ликер (биттер) крепостью 35%, созданный на основе трав, кореньев и пряностей.","kk":"Шөптер, тамырлар мен дәмдеуіштер негізінде жасалған әйгілі неміс күшті ликері (биттер), күштілігі 35%."},"img":"/menu-img/df80f9fd-9d32-483b-ba07-42ceb1e9e0d2-t.jpg","imgFull":"/menu-img/df80f9fd-9d32-483b-ba07-42ceb1e9e0d2.jpg"}],"id":"8c199bd4-9ce9-44c0-b3f1-b9c624fff8e2"},{"title":{"ru":"Игристые вина","kk":"Жарқыраған шараптар","en":"Sparkling Wine"},"items":[{"name":{"ru":"MARTINI Prosecco DOC 0,75 л.","kk":"MARTINI Prosecco DOC 0,75 л"},"price":"17 890","id":"5d53704e-fc48-3599-bcd9-dd96cb3cf4cf","price2":"3 510","desc":{"ru":"750 мл / 150 мл. Сухое итальянское игристое вино из региона Венето с крепостью 11,5%, обладающее свежим фруктовым вкусом и ароматом белых цветов.","kk":"750 мл / 150 мл. Венето өңірінің құрғақ итальяндық шампан шарабы, күштілігі 11,5%: балғын жеміс дәмі мен ақ гүлдердің иісі бар."},"img":"/menu-img/5d53704e-fc48-3599-bcd9-dd96cb3cf4cf-t.jpg","imgFull":"/menu-img/5d53704e-fc48-3599-bcd9-dd96cb3cf4cf.jpg"},{"name":{"ru":"MARTINI Asti DOCG 0,75 л","kk":"MARTINI Asti DOCG 0,75 л"},"price":"17 890","id":"798436ab-b781-30ff-ad63-3c7f853582be","desc":{"ru":"750 мл. Итальянское сладкое игристое вино из региона Пьемонт, созданное из винограда белый мускат.","kk":"750 мл. Пьемонт өңірінің ақ мускат жүзімінен жасалған итальяндық тәтті шампан шарабы."},"img":"/menu-img/798436ab-b781-30ff-ad63-3c7f853582be-t.jpg","imgFull":"/menu-img/798436ab-b781-30ff-ad63-3c7f853582be.jpg"},{"name":{"ru":"MARTINI Brut 0,75 л","kk":"MARTINI Brut 0,75 л"},"price":"17 890","id":"c34a0781-54d8-348b-b7e2-aaad563206fc","desc":{"ru":"750 мл. Белое сухое игристое вино из Италии крепостью 11,5%, обладающее мягким, освежающим вкусом с тонами зеленого яблока, груши и миндаля.","kk":"750 мл. Италиядан келген ақ құрғақ шампан шарабы, күштілігі 11,5%: жасыл алма, алмұрт және бадам реңктері бар жұмсақ, сергітетін дәм."},"img":"/menu-img/c34a0781-54d8-348b-b7e2-aaad563206fc-t.jpg","imgFull":"/menu-img/c34a0781-54d8-348b-b7e2-aaad563206fc.jpg"}],"id":"983c9ea5-439e-35ee-a1c4-4ab484ff191f"},{"title":{"ru":"Вина Грузии","kk":"Грузин шарабы","en":"Georgian Wine"},"items":[{"name":{"ru":"Твиши Marani ( бел.п/сл)","kk":"Твиши Marani"},"price":"3 949","id":"3a61cc50-afd0-32c6-88f9-561f027733c6","price2":"19 789","desc":{"ru":"150 мл / 750 мл. Природно-полусладкое белое вино из Грузии, произведенное в микрозоне Твиши из винограда сорта Цоликаури. Сочетается с легкими закусками, фруктами, сырами, белым мясом и птицей.","kk":"150 мл / 750 мл. Бұл грузин жартылай тәтті ақ шарабы келесі тағамдармен жақсы үйлеседі: • ірімшіктермен • жемістермен • жеңіл десерттермен • немесе жай ғана тамақсыз \"ләззат\" шарабы ретінде"},"img":"/menu-img/3a61cc50-afd0-32c6-88f9-561f027733c6-t.jpg","imgFull":"/menu-img/3a61cc50-afd0-32c6-88f9-561f027733c6.jpg"},{"name":{"ru":"Киндзмараули Marani (бокал кр.пс)","kk":"Киндзмараули Marani (бокал, қызыл, жартылай тәтті)"},"price":"3 949","id":"52a8573f-cf86-35eb-9545-c65c4975b57d","price2":"19 789","desc":{"ru":"150 мл / 750 мл. Знаменитое грузинское красное полусладкое вино, производится из 100% автохтонного сорта винограда Саперави. Идеально подходит к: фруктовой выпечке, мягким или пикантным сырам, Нежирное мясо, приготовленное на гриле","kk":"150 мл / 750 мл. Бұл грузин қызыл жартылай тәтті шарабы. Мыналармен жақсы үйлеседі: • жемістер мен жидектер 🍓 • десерттер 🍰 • жұмсақ ірімшіктер 🧀 • жеңіл тағамдар"},"img":"/menu-img/52a8573f-cf86-35eb-9545-c65c4975b57d-t.jpg","imgFull":"/menu-img/52a8573f-cf86-35eb-9545-c65c4975b57d.jpg"},{"name":{"ru":"Саперави Marani (кр/сух)","kk":"Саперави Marani"},"price":"14 200","id":"99354eee-4a40-30de-acf4-1e929e4ed814","desc":{"ru":"750 мл. красное сухое грузинское вино с насыщенным вкусом спелых тёмных ягод и лёгкими пряными нотами. идеально к мясным блюдам, баранине, шашлыку и блюдам грузинской кухни.","kk":"750 мл. Бай, піскен, қою жидек дәмі мен жеңіл ащы ноталары бар құрғақ қызыл грузин шарабы. Ет тағамдарымен, қой етімен, кәуаппен және грузин тағамдарымен тамаша үйлеседі."},"img":"/menu-img/99354eee-4a40-30de-acf4-1e929e4ed814-t.jpg","imgFull":"/menu-img/99354eee-4a40-30de-acf4-1e929e4ed814.jpg"},{"name":{"ru":"Цинандали Marani (Бел/ сух)","kk":"Цинандали Marani"},"price":"14 200","id":"6b5d2ad4-f313-38cc-a4b0-fd9d7f4fd904","desc":{"ru":"750 мл. грузинское белое сухое вино из традиционного купажа сортов Rkatsiteli и Mtsvane. Гастрономия: отлично с рыбой, морепродуктами, белым мясом и лёгкими салатами.","kk":"750 мл. Грузин құрғақ ақ шарабы Гастрономия: Балық, теңіз өнімдері, ақ ет және жеңіл салаттармен тамаша үйлеседі."},"img":"/menu-img/6b5d2ad4-f313-38cc-a4b0-fd9d7f4fd904-t.jpg","imgFull":"/menu-img/6b5d2ad4-f313-38cc-a4b0-fd9d7f4fd904.jpg"},{"name":{"ru":"Вино Тетри Алазанская долина бел. п/сл","kk":"Тетри «Алазан аңғары» шарабы, ақ, жартылай тәтті"},"price":"10 900","id":"1cf8b842-754b-48a5-b8fb-127ba3d11336","desc":{"ru":"0,75 мл. Традиционное грузинское вино из Кахетинского региона, обладающее мягким фруктовым вкусом. Хорошо сочетается с легкими закусками, и блюдами из белого мяса или рыбы.","kk":"0,75 мл. Кахети өңірінің жұмсақ жеміс дәмі бар дәстүрлі грузин шарабы. Жеңіл тағамдармен, ақ ет және балық тағамдарымен жақсы үйлеседі."},"img":"/menu-img/1cf8b842-754b-48a5-b8fb-127ba3d11336-t.jpg","imgFull":"/menu-img/1cf8b842-754b-48a5-b8fb-127ba3d11336.jpg"},{"name":{"ru":"Вино Тетри Алазанская долина красное . п/сл","kk":"Тетри «Алазан аңғары» шарабы, қызыл, жартылай тәтті"},"price":"10 900","id":"6596d549-3201-45ed-8296-5fffe704a2bf","desc":{"ru":"0,75 мл. Традиционное грузинское столовое вино из региона Кахетия. Хорошо сочетается с мясными блюдами, шашлыком, мясом на гриле.","kk":"0,75 мл. Кахети өңірінің дәстүрлі грузин асханалық шарабы. Ет тағамдарымен, кәуаппен, грильде пісірілген етпен жақсы үйлеседі."},"img":"/menu-img/6596d549-3201-45ed-8296-5fffe704a2bf-t.jpg","imgFull":"/menu-img/6596d549-3201-45ed-8296-5fffe704a2bf.jpg"}],"id":"c004b8e7-c005-3bba-b08d-86f542adf0dd"},{"title":{"ru":"Вина Франции","kk":"Франция шараптары","en":"French Wine"},"items":[{"name":{"ru":"Lamblin Chardonnay (бел.сух)","kk":"Lamblin Chardonnay"},"price":"19 500","id":"7236ae32-a6d0-386f-8cf4-2f32f1b279fd","desc":{"ru":"750 мл. белое сухое вино с элегантным ароматом яблока, груши и лёгкими цитрусовыми нотами. Во вкусе свежее, мягкое, хорошо сбалансированное. Гастрономия: подходит к рыбе, морепродуктам, курице, пасте с сливочными соусами и лёгким закускам.","kk":"750 мл. Алма мен алмұрттың талғампаз хош иістері мен жеңіл цитрус ноталары бар құрғақ ақ шарап. Дәмі балғын, жұмсақ және жақсы теңдестірілген. Балық, теңіз өнімдері, тауық еті, кілегейлі тұздықтары бар макарон және жеңіл тағамдармен жақсы үйлеседі."},"img":"/menu-img/7236ae32-a6d0-386f-8cf4-2f32f1b279fd-t.jpg","imgFull":"/menu-img/7236ae32-a6d0-386f-8cf4-2f32f1b279fd.jpg"},{"name":{"ru":"Lamblin Merlot-Cabernet (кр.сух)","kk":"Lamblin Merlot-Cabernet"},"price":"19 500","id":"f64bb188-70ab-38e7-8d66-e2cb03e47fad","desc":{"ru":"750 мл. красное сухое вино с ароматами чёрной смородины, спелой вишни и лёгкими пряными нотами. Во вкусе насыщенное, с выраженными танинами и сбалансированной кислотностью. Гастрономия: идеально к красному мясу, стейкам, шашлыку, баранине и выдержанным сырам.","kk":"750 мл. Қарақат, піскен шие және жеңіл ащы ноталары бар құрғақ қызыл шарап. Дәмі бай, айқын таниндер мен теңгерімді қышқылдыққа ие. Тағамдық үйлесім: Қызыл ет, стейктер, кәуап, қой еті және ескі ірімшіктермен тамаша үйлеседі."},"img":"/menu-img/f64bb188-70ab-38e7-8d66-e2cb03e47fad-t.jpg","imgFull":"/menu-img/f64bb188-70ab-38e7-8d66-e2cb03e47fad.jpg"}],"id":"c99e0d95-0980-3d23-b992-502b6e18172b"},{"title":{"ru":"Вина Италии","kk":"Италия шараптары","en":"Italian Wine"},"items":[{"name":{"ru":"Cielo Pinot Grigio (бел.псух)","kk":"Cielo Pinot Grigio"},"price":"18 900","id":"1de683a5-5e1b-317a-8205-1d6db7d0ecb4","desc":{"ru":"750 мл. Легкое, освежающее итальянское белое вино. Сочетается с легкими блюдами из рыбы, морепродуктами, белым мясом и овощными салатами.","kk":"750 мл. Жеңіл, сергітетін итальяндық ақ шарап. Жеңіл балық тағамдарымен, теңіз өнімдерімен, ақ етпен және көкөніс салаттарымен үйлеседі."},"img":"/menu-img/1de683a5-5e1b-317a-8205-1d6db7d0ecb4-t.jpg","imgFull":"/menu-img/1de683a5-5e1b-317a-8205-1d6db7d0ecb4.jpg"},{"name":{"ru":"Pinot Grigio Blush (роз.псух)","kk":"Pinot Grigio Blush"},"price":"18 900","id":"707d4615-6dc8-3c2b-aad6-210abb2ac34e","desc":{"ru":"750 мл. лёгкое, свежее итальянское полусухое розовое вино. гастрономия : Салаты (особенно с птицей или морепродуктами), морепродукты (креветки, рыба на пару или гриле), белое мясо (курица), паста с легкими томатными соусами, ризотто.","kk":"750 мл. Жеңіл, балғын итальяндық жартылай құрғақ раушан шарабы. Гастрономия: Салаттар (әсіресе құс еті немесе теңіз өнімдерімен), теңіз өнімдері (асшаяндар, буға пісірілген немесе грильде пісірілген балық), ақ ет (тауық еті), жеңіл қызанақ тұздығы қосылған макарон, ризотто."},"img":"/menu-img/707d4615-6dc8-3c2b-aad6-210abb2ac34e-t.jpg","imgFull":"/menu-img/707d4615-6dc8-3c2b-aad6-210abb2ac34e.jpg"}],"id":"ffa46716-b5be-398c-bcb9-77a2fcedaae1"},{"title":{"ru":"Вина Чили","kk":"Чили шараптары","en":"Chilean Wine"},"items":[{"name":{"ru":"Sanama Reserva Sauvignon Blanc (бел.сух)","kk":"Sanama Reserva Sauvignon Blanc"},"price":"17 700","id":"95b30f7a-09bc-38d0-8c75-b2e2d605784f","desc":{"ru":"750 мл. обычная цена 19 500. белое сухое вино с яркими ароматами цитрусовых, зелёного яблока и свежих трав. Во вкусе освежающее, с живой кислотностью и чистым послевкусием. Гастрономия: идеально к рыбе, морепродуктам, салатам, козьему сыру и лёгким закускам.","kk":"750 мл. бұрынғы бағасы 19 500. Цитрус, жасыл алма және жаңа піскен шөптердің жарқын хош иісі бар құрғақ ақ шарап. Таңдайды сергітеді, қышқылдығымен және тазалығымен ерекшеленеді. Жұптастыру: Балық, теңіз өнімдері, салаттар, ешкі ірімшігі және жеңіл тағамдармен тамаша үйлеседі."},"img":"/menu-img/95b30f7a-09bc-38d0-8c75-b2e2d605784f-t.jpg","imgFull":"/menu-img/95b30f7a-09bc-38d0-8c75-b2e2d605784f.jpg"},{"name":{"ru":"Sanama Cabernet Sauvignon (кр.сух)","kk":"Sanama Cabernet Sauvignon"},"price":"17 700","id":"f338dc96-f515-3a71-b66b-501f85bbe7f4","desc":{"ru":"750 мл. обычная цена 19 500. красное сухое вино с ароматами спелых тёмных ягод, сливы и лёгкими перечными нотами. Во вкусе мягкое, округлое, с бархатными танинами и пряным послевкусием. Гастрономия: отлично подходит к красному мясу, баранине, тушёным блюдам, грилю и выдержанным сырам.","kk":"750 мл. бұрынғы бағасы 19 500. Піскен қара жидектер, қара өрік және жеңіл бұрыш ноталары бар құрғақ қызыл шарап. Дәмі жұмсақ және дөңгелек, барқыттай таниндермен және ащы дәммен ерекшеленеді. Тағамдық үйлесім: Қызыл ет, қой еті, бұқтырылған тағамдар, грильде пісірілген ет және ескі ірімшіктермен тамаша үйлеседі."},"img":"/menu-img/f338dc96-f515-3a71-b66b-501f85bbe7f4-t.jpg","imgFull":"/menu-img/f338dc96-f515-3a71-b66b-501f85bbe7f4.jpg"}],"id":"f5b8d93f-d095-3eb7-815b-c38aa59be926"},{"title":{"ru":"Лимонады","kk":"Лимонадтар","en":"Lemonades"},"items":[{"name":{"ru":"Мятно - Апельсиновый Джульеп 1л","kk":"Жалбыз-апельсин джулебі 1 л"},"price":"2 750","id":"7d4c8121-dbda-31f9-ae83-b628d45592d1","desc":{"ru":"1 л","kk":"1 л"},"img":"/menu-img/7d4c8121-dbda-31f9-ae83-b628d45592d1-t.jpg","imgFull":"/menu-img/7d4c8121-dbda-31f9-ae83-b628d45592d1.jpg"},{"name":{"ru":"Авторский от бармена (манго / маракуйя)","kk":"Бармен лимонады (манго / маракуйя)"},"price":"2 750","id":"f2b6cdb4-dcdf-30cd-b53b-1a3e48b195d5","desc":{"ru":"1 л","kk":"1 л"},"img":"/menu-img/f2b6cdb4-dcdf-30cd-b53b-1a3e48b195d5-t.jpg","imgFull":"/menu-img/f2b6cdb4-dcdf-30cd-b53b-1a3e48b195d5.jpg"},{"name":{"ru":"Ягодный 1л","kk":"Жидек лимонады 1 л"},"price":"2 750","id":"8c6c9f88-52da-3efe-abf8-67f48fa1c27a","desc":{"ru":"1 л","kk":"1 л"},"img":"/menu-img/8c6c9f88-52da-3efe-abf8-67f48fa1c27a-t.jpg","imgFull":"/menu-img/8c6c9f88-52da-3efe-abf8-67f48fa1c27a.jpg"},{"name":{"ru":"Тархун лимонад 1 л","kk":"Тархун лимонады 1 л"},"price":"2 750","id":"0d82f3b0-d54e-3fbc-8ceb-f6d1c40f6548","desc":{"ru":"1 л","kk":"1 л"},"img":"/menu-img/0d82f3b0-d54e-3fbc-8ceb-f6d1c40f6548-t.jpg","imgFull":"/menu-img/0d82f3b0-d54e-3fbc-8ceb-f6d1c40f6548.jpg"},{"name":{"ru":"Яблоко-киви 1л","kk":"Алма-киви 1 л"},"price":"2 750","id":"5b0303d3-cb02-3cec-96c1-a14627ad8b34","desc":{"ru":"1 л","kk":"1 л"},"img":"/menu-img/5b0303d3-cb02-3cec-96c1-a14627ad8b34-t.jpg","imgFull":"/menu-img/5b0303d3-cb02-3cec-96c1-a14627ad8b34.jpg"},{"name":{"ru":"Цитрусовый 1л","kk":"Цитрус лимонады 1 л"},"price":"2 750","id":"80215f75-9b1d-3f77-af61-152035e76b27","desc":{"ru":"1 л","kk":"1 л"},"img":"/menu-img/80215f75-9b1d-3f77-af61-152035e76b27-t.jpg","imgFull":"/menu-img/80215f75-9b1d-3f77-af61-152035e76b27.jpg"},{"name":{"ru":"Тропический заряд 0,33 мл","kk":"Тропикалық қуат 0,33 мл"},"price":"1 790","id":"dc1eede9-602a-4e6a-851f-77159ab641e1","img":"/menu-img/dc1eede9-602a-4e6a-851f-77159ab641e1-t.jpg","imgFull":"/menu-img/dc1eede9-602a-4e6a-851f-77159ab641e1.jpg"},{"name":{"ru":"Грейпфрутовый драйв 0,33 мл","kk":"Грейпфрут драйвы 0,33 мл"},"price":"1 790","id":"bc05aa45-02cf-4701-8f51-83b98daa8ff4","img":"/menu-img/bc05aa45-02cf-4701-8f51-83b98daa8ff4-t.jpg","imgFull":"/menu-img/bc05aa45-02cf-4701-8f51-83b98daa8ff4.jpg"}],"id":"3df0ff93-74fd-35bc-8ffd-2a9c97fc7437"},{"title":{"ru":"Безалкогольные напитки","kk":"Сусындар","en":"Soft Drinks"},"items":[{"name":{"ru":"Минеральная вода Боржоми 0,33 ml","kk":"Боржоми минералды суы 0,33 мл"},"price":"1 990","id":"a46208bc-8ca2-483b-a495-a7d0663115b2","desc":{"ru":"0.33 мл. Знаменитая природная гидрокарбонатная натриевая минеральная вода вулканического происхождения из Грузии.","kk":"0.33 мл. Грузиядан шыққан жанартау тектес атақты табиғи гидрокарбонатты натрий минералды суы."},"img":"/menu-img/a46208bc-8ca2-483b-a495-a7d0663115b2-t.jpg","imgFull":"/menu-img/a46208bc-8ca2-483b-a495-a7d0663115b2.jpg"},{"name":{"ru":"TURAN Природная вода 0,25 ml","kk":"TURAN табиғи суы 0,25 мл"},"price":"880","id":"38d733ea-4014-3272-a4a7-2e85e575cce0","price2":"1 400","desc":{"ru":"250 мл / 1 л. Легкая артезианская вода с мягким вкусом и сбалансированным минеральным составом.","kk":"250 мл / 1 л. Жұмсақ дәмі мен теңгерімді минералды құрамы бар жеңіл артезиан суы."},"img":"/menu-img/38d733ea-4014-3272-a4a7-2e85e575cce0-t.jpg","imgFull":"/menu-img/38d733ea-4014-3272-a4a7-2e85e575cce0.jpg"},{"name":{"ru":"Cola \\ Sprite 0.25 ml"},"price":"990","id":"bc973602-52f5-3714-84c3-322dd6d2332f","desc":{"ru":"250 мл. Два легендарных вкуса. Никаких сложных решений.","kk":"250 мл. Екі аңызға айналған дәм. Ешқандай қиын таңдау жоқ."},"img":"/menu-img/bc973602-52f5-3714-84c3-322dd6d2332f-t.jpg","imgFull":"/menu-img/bc973602-52f5-3714-84c3-322dd6d2332f.jpg"},{"name":{"ru":"Coca-Cola 1 L"},"price":"2 000","id":"7c0a3b34-7764-34d6-9e8a-6a06fd69d277","desc":{"ru":"1 л. Становится вдвойне вкусней, если добавить алкоголь;)","kk":"1 л. Алкоголь қоссаңыз, екі есе дәмді болады ;)"},"img":"/menu-img/7c0a3b34-7764-34d6-9e8a-6a06fd69d277-t.jpg","imgFull":"/menu-img/7c0a3b34-7764-34d6-9e8a-6a06fd69d277.jpg"},{"name":{"ru":"Tonic Schweppes 0,45 ml"},"price":"1 400","id":"72a531d1-5daa-3eeb-9d1c-19c98071189c","desc":{"ru":"Легендарный сильногазированный безалкогольный напиток с характерным горьковато-кислым вкусом и содержанием хинина.","kk":"Хинин қосылған, ащылау-қышқыл дәмі бар аңызға айналған қатты газдалған алкогольсіз сусын."},"img":"/menu-img/72a531d1-5daa-3eeb-9d1c-19c98071189c-t.jpg","imgFull":"/menu-img/72a531d1-5daa-3eeb-9d1c-19c98071189c.jpg"},{"name":{"ru":"Red Bull 0,25 ml"},"price":"1 650","id":"67f797a1-04ca-3c97-b944-a22242fd3bad","desc":{"ru":"Культовый газированный энергетический напиток в узнаваемой серебристо-синей жестяной банке с ключом.","kk":"Кілті бар танымал күміс-көк қаңылтыр құтыдағы әйгілі газдалған энергетикалық сусын."},"img":"/menu-img/67f797a1-04ca-3c97-b944-a22242fd3bad-t.jpg","imgFull":"/menu-img/67f797a1-04ca-3c97-b944-a22242fd3bad.jpg"},{"name":{"ru":"Соки в ассортименте 1l","kk":"Түрлі шырындар 1 л"},"price":"880","id":"3ebfb9ea-59db-3bb9-a3bb-3e6769421083","price2":"2 200","desc":{"ru":"250 мл / 1 л. Яркий вкус, сочные фрукты и никакой скуки. Выбирай свой вкус, и добавляй немного фруктового настроения в свой вечер.","kk":"250 мл / 1 л. Жарқын дәм, шырынды жемістер және ешқандай іш пысу жоқ. Өз дәміңізді таңдап, кешіңізге жемісті көңіл-күй қосыңыз."},"img":"/menu-img/3ebfb9ea-59db-3bb9-a3bb-3e6769421083-t.jpg","imgFull":"/menu-img/3ebfb9ea-59db-3bb9-a3bb-3e6769421083.jpg"},{"name":{"ru":"Морс домашний (клюква) -0,25 ml","kk":"Үй морсы (мүкжидек) 0,25 мл"},"price":"799","id":"acdaf19c-8950-383e-9426-3e21d5fa25ba","price2":"1 990","desc":{"ru":"250 мл / 1 л. Традиционный, полезный и освежающий ягодный напиток насыщенного рубинового цвета с приятной кислинкой.","kk":"250 мл / 1 л. Жағымды қышқылдығы бар, қою лағыл түсті дәстүрлі, пайдалы әрі сергітетін жидек сусыны."},"img":"/menu-img/acdaf19c-8950-383e-9426-3e21d5fa25ba-t.jpg","imgFull":"/menu-img/acdaf19c-8950-383e-9426-3e21d5fa25ba.jpg"}],"id":"58aa1ef6-75b7-37cf-b915-5592724a10c3"},{"title":{"ru":"Кофе","kk":"Кофе","en":"Coffee"},"items":[{"name":{"ru":"Americano"},"price":"1 100","id":"28d5c6c1-e6ed-378e-b176-6a8d20743e02","desc":{"ru":"Кофейный напиток, представляющий собой порцию эспрессо, разбавленную горячей водой.","kk":"Ыстық сумен сұйылтылған бір порция эспрессо."},"img":"/menu-img/28d5c6c1-e6ed-378e-b176-6a8d20743e02-t.jpg","imgFull":"/menu-img/28d5c6c1-e6ed-378e-b176-6a8d20743e02.jpg"},{"name":{"ru":"Espresso"},"price":"1 100","id":"144215df-e1e9-3b09-87cd-b3fa31a173d5","desc":{"ru":"Крепкий черный кофейный напиток объемом 25–30 мл","kk":"Көлемі 25–30 мл болатын күшті қара кофе"},"img":"/menu-img/144215df-e1e9-3b09-87cd-b3fa31a173d5-t.jpg","imgFull":"/menu-img/144215df-e1e9-3b09-87cd-b3fa31a173d5.jpg"},{"name":{"ru":"Cappuccino"},"price":"1 400","id":"6d6b5e22-8b75-30da-b007-56eaf510fad6","desc":{"ru":"Итальянский кофейный напиток на основе эсспрессо с добавлением подогретого молока и плотной мелкодисперсной молочной пены.","kk":"Жылытылған сүт пен қою ұсақ сүт көбігі қосылған эспрессо негізіндегі итальяндық кофе сусыны."},"img":"/menu-img/6d6b5e22-8b75-30da-b007-56eaf510fad6-t.jpg","imgFull":"/menu-img/6d6b5e22-8b75-30da-b007-56eaf510fad6.jpg"},{"name":{"ru":"Latte"},"price":"1 400","id":"9f0d3284-46de-3342-910d-58b34c234452","desc":{"ru":"Кофейно-молочный напиток мягкого вкуса, который состоит из порции эспрессо, большого количества горячего молока и нежной молочной пены.","kk":"Бір порция эспрессо, көп ыстық сүт және нәзік сүт көбігінен тұратын жұмсақ дәмді кофе-сүт сусыны."},"img":"/menu-img/9f0d3284-46de-3342-910d-58b34c234452-t.jpg","imgFull":"/menu-img/9f0d3284-46de-3342-910d-58b34c234452.jpg"},{"name":{"ru":"Aroma Latte"},"price":"1 500","id":"4eb224ba-dd1b-38ef-9b79-f03f4e952dd4","desc":{"ru":"Мягкий слоистый кофейный напиток на основе эспрессо, большого количества молока, нежной пенки и сладкой ароматической добавки.","kk":"Эспрессо, көп сүт, нәзік көбік және тәтті хош иісті қоспа негізіндегі жұмсақ қабатты кофе сусыны."},"img":"/menu-img/4eb224ba-dd1b-38ef-9b79-f03f4e952dd4-t.jpg","imgFull":"/menu-img/4eb224ba-dd1b-38ef-9b79-f03f4e952dd4.jpg"}],"id":"fbee565d-d2ba-369d-a129-5e36d47ce5d8"},{"title":{"ru":"Чай","kk":"Шай","en":"Tea"},"items":[{"name":{"ru":"Чай черный листовой","kk":"Жапырақты қара шай"},"price":"1 320","id":"7452bb23-ae68-31b7-bf38-94cecedd1dd1","desc":{"ru":"800 мл. Полностью ферментированный натуральный напиток из цельных или частично измельченных чайных листьев, обладающий глубоким насыщенным вкусом и ароматом.","kk":"800 мл. Тұтас немесе жартылай ұсақталған шай жапырақтарынан жасалған, толық ферменттелген табиғи сусын: дәмі мен иісі терең әрі бай."},"img":"/menu-img/7452bb23-ae68-31b7-bf38-94cecedd1dd1-t.jpg","imgFull":"/menu-img/7452bb23-ae68-31b7-bf38-94cecedd1dd1.jpg"},{"name":{"ru":"Чай зеленый листовой","kk":"Жапырақты жасыл шай"},"price":"1 320","id":"1ff74e3b-5024-3cd8-8c62-325f26093cee","desc":{"ru":"800 мл. Натуральный чай из цельных или аккуратно скрученных листьев растения Camellia sinensis, которые прошли минимальную ферментацию","kk":"800 мл. Ең аз ферменттеуден өткен Camellia sinensis өсімдігінің тұтас немесе ұқыпты бұралған жапырақтарынан жасалған табиғи шай"},"img":"/menu-img/1ff74e3b-5024-3cd8-8c62-325f26093cee-t.jpg","imgFull":"/menu-img/1ff74e3b-5024-3cd8-8c62-325f26093cee.jpg"},{"name":{"ru":"Чай зеленый с жасмином","kk":"Жасмин қосылған жасыл шай"},"price":"1 980","id":"a3829a89-cf77-364f-bbe3-574f1f50bb84","desc":{"ru":"800 мл. Ароматный напиток из зелёного чая, пропитанный нежным цветочным запахом.","kk":"800 мл. Нәзік гүл иісі сіңген жасыл шайдан жасалған хош иісті сусын."},"img":"/menu-img/a3829a89-cf77-364f-bbe3-574f1f50bb84-t.jpg","imgFull":"/menu-img/a3829a89-cf77-364f-bbe3-574f1f50bb84.jpg"},{"name":{"ru":"Ташкентский чай","kk":"Ташкент шайы"},"price":"1 980","id":"bd00da7d-b8ed-3576-81e8-d62c61ea868a","desc":{"ru":"800 мл. Освежающий и ароматный чайный напиток из смеси черного и зеленого чая с добавлением цитрусов, мяты и меда.","kk":"800 мл. Цитрус, жалбыз және бал қосылған қара және жасыл шай қоспасынан жасалған сергітетін хош иісті шай сусыны."},"img":"/menu-img/bd00da7d-b8ed-3576-81e8-d62c61ea868a-t.jpg","imgFull":"/menu-img/bd00da7d-b8ed-3576-81e8-d62c61ea868a.jpg"},{"name":{"ru":"Марокканский чай.","kk":"Марокко шайы"},"price":"1 980","id":"7c4639a5-9faf-3d09-81ef-2939e507eac7","desc":{"ru":"800 мл. Традиционный освежающий и бодрящий напиток из зеленого чая, большого количества свежей мяты и сахара.","kk":"800 мл. Жасыл шай, көп балғын жалбыз және қанттан жасалған дәстүрлі сергітетін әрі сергек ететін сусын."},"img":"/menu-img/7c4639a5-9faf-3d09-81ef-2939e507eac7-t.jpg","imgFull":"/menu-img/7c4639a5-9faf-3d09-81ef-2939e507eac7.jpg"},{"name":{"ru":"Имбирно – апельсиновый","kk":"Зімбір-апельсин шайы"},"price":"1 980","id":"fca19216-291d-3b36-bb26-e56235c2436d","desc":{"ru":"800 мл. Согревающий, витаминный и бодрящий напиток с ярким цитрусовым ароматом и приятной пикантной остринкой.","kk":"800 мл. Жарқын цитрус иісі мен жағымды ащылау дәмі бар жылытатын, дәрумендік әрі сергітетін сусын."},"img":"/menu-img/fca19216-291d-3b36-bb26-e56235c2436d-t.jpg","imgFull":"/menu-img/fca19216-291d-3b36-bb26-e56235c2436d.jpg"},{"name":{"ru":"Малиновый с розмарином","kk":"Розмарин қосылған таңқурай шайы"},"price":"1 980","id":"c3f78070-03a6-3dc7-b76b-eaecac4e1497","desc":{"ru":"800 мл. Яркий, согревающий и витаминный напиток с гармоничным сочетанием сладких ягод и пряных хвойных ноток.","kk":"800 мл. Тәтті жидектер мен дәмдеуішті қылқан жапырақты реңктер үйлескен жарқын, жылытатын әрі дәрумендік сусын."},"img":"/menu-img/c3f78070-03a6-3dc7-b76b-eaecac4e1497-t.jpg","imgFull":"/menu-img/c3f78070-03a6-3dc7-b76b-eaecac4e1497.jpg"},{"name":{"ru":"Авторский от бармена","kk":"Бармен шайы"},"price":"1 980","id":"9c9af836-fc57-3158-ab00-809b5133d117","desc":{"ru":"800 мл. Яркий, согревающий и невероятно полезный витаминный напиток с насыщенным цитрусово-пряным ароматом и приятной кислинкой облепихи.","kk":"800 мл. Цитрус-дәмдеуіш иісі мен шырғанақтың жағымды қышқылдығы бар жарқын, жылытатын әрі өте пайдалы дәрумендік сусын."},"img":"/menu-img/9c9af836-fc57-3158-ab00-809b5133d117-t.jpg","imgFull":"/menu-img/9c9af836-fc57-3158-ab00-809b5133d117.jpg"},{"name":{"ru":"Чай","kk":"Шай"},"price":"220","id":"ad5eb0ce-2dac-3145-a316-64bc8de6b4fa","desc":{"ru":"чашка. Дает крепкий настой с насыщенным цветом, приятным ароматом и сбалансированным вкусом.","kk":"чашка. Түсі қою, иісі жағымды, дәмі теңгерімді күшті шай."},"img":"/menu-img/ad5eb0ce-2dac-3145-a316-64bc8de6b4fa-t.jpg","imgFull":"/menu-img/ad5eb0ce-2dac-3145-a316-64bc8de6b4fa.jpg"}],"id":"06065acb-9a50-3297-9e4a-2020d838c821"},{"title":{"ru":"К чаю","kk":"Шайға қосымша","en":"With Tea"},"items":[{"name":{"ru":"Молоко","kk":"Сүт"},"price":"300","id":"cf74c26d-760d-3f17-8945-713a59960a4e","img":"/menu-img/cf74c26d-760d-3f17-8945-713a59960a4e-t.jpg","imgFull":"/menu-img/cf74c26d-760d-3f17-8945-713a59960a4e.jpg"},{"name":{"ru":"Шоколад «Казахстан»","kk":"«Казахстан» шоколады"},"price":"2 000","id":"481ce26b-da91-3b98-b72c-89d9a7da3642","desc":{"ru":"Заменитый молочный шоколад с высоким содержанием какао (не менее 45%) и нежным ванильным ароматом","kk":"Какао мөлшері жоғары (кемінде 45%) және нәзік ваниль иісі бар әйгілі сүтті шоколад"},"img":"/menu-img/481ce26b-da91-3b98-b72c-89d9a7da3642-t.jpg","imgFull":"/menu-img/481ce26b-da91-3b98-b72c-89d9a7da3642.jpg"},{"name":{"ru":"Лимон","kk":"Лимон"},"price":"300","id":"5d063520-25e7-3c7d-a7f6-78917d9baeda","img":"/menu-img/5d063520-25e7-3c7d-a7f6-78917d9baeda-t.jpg","imgFull":"/menu-img/5d063520-25e7-3c7d-a7f6-78917d9baeda.jpg"},{"name":{"ru":"Сироп на выбор","kk":"Таңдауыңызша шәрбат"},"price":"300","id":"75a5d019-b5df-319b-b895-83a53026b11d","desc":{"ru":"Казахстанские интонационные сиропы с содержанием стевии 50% и высокой концентрацией натуральных соков и пюре.","kk":"Стевия мөлшері 50% және табиғи шырындар мен езбелер концентрациясы жоғары қазақстандық шәрбаттар."},"img":"/menu-img/75a5d019-b5df-319b-b895-83a53026b11d-t.jpg","imgFull":"/menu-img/75a5d019-b5df-319b-b895-83a53026b11d.jpg"},{"name":{"ru":"Мёд","kk":"Бал"},"price":"300","id":"9b8c5d31-dec2-3953-90fb-36deddc433be","desc":{"ru":"Сделай жизнь слаще :)","kk":"Өміріңізді тәттірек етіңіз :)"},"img":"/menu-img/9b8c5d31-dec2-3953-90fb-36deddc433be-t.jpg","imgFull":"/menu-img/9b8c5d31-dec2-3953-90fb-36deddc433be.jpg"}],"id":"57a5bbd8-7ae5-38ce-b478-6bcf0cf8f2e2"},{"title":{"ru":"Cocktail Menu","kk":"Коктейльдер","en":"Cocktails"},"items":[{"name":{"ru":"Lawson`s & Cola"},"price":"2 550","id":"54973036-778f-338a-aa25-cc426c71fd9d","desc":{"ru":"ШОТЛАНДСКИЙ ВИСКИ С КУСКОВЫМ ЛЬДОМ И КОЛОЙ","kk":"Кесек мұз бен кола қосылған шотланд вискиі"},"img":"/menu-img/54973036-778f-338a-aa25-cc426c71fd9d-t.jpg","imgFull":"/menu-img/54973036-778f-338a-aa25-cc426c71fd9d.jpg"},{"name":{"ru":"Red Bull & Jager"},"price":"3 090","id":"739f9582-f028-32f8-9b54-710416de60ab","desc":{"ru":"МИКС С ПРОСТЫМ СОСТАВОМ: БИТТЕР ЕГЕРМЕЙСТЕР И ЭНЕРГЕТИК РЕД БУЛЛ","kk":"Қарапайым құрамды микс: Jägermeister биттері және Red Bull энергетигі"},"img":"/menu-img/739f9582-f028-32f8-9b54-710416de60ab-t.jpg","imgFull":"/menu-img/739f9582-f028-32f8-9b54-710416de60ab.jpg"},{"name":{"ru":"Red Bull & Vodka"},"price":"2 590","id":"996b60ca-12fb-3b69-9969-723629c445f0","desc":{"ru":"ОДИН ИЗ САМЫХ «ВЗРЫВНЫХ» КОКТЕЙЛЕЙ С ВОДКОЙ И ЭНЕРГЕТИКОМ","kk":"Арақ пен энергетик қосылған ең «жарылғыш» коктейльдердің бірі"},"img":"/menu-img/996b60ca-12fb-3b69-9969-723629c445f0-t.jpg","imgFull":"/menu-img/996b60ca-12fb-3b69-9969-723629c445f0.jpg"},{"name":{"ru":"OAK & Cola"},"price":"2 590","id":"bc287c9c-f00c-3db6-b69e-7ab4307f5ed0","desc":{"ru":"КОКТЕЙЛЬ НА ОСНОВЕ РОМА BACARDI OAKHEART С КОЛОЙ","kk":"Bacardi Oakheart ромы мен кола негізіндегі коктейль"},"img":"/menu-img/bc287c9c-f00c-3db6-b69e-7ab4307f5ed0-t.jpg","imgFull":"/menu-img/bc287c9c-f00c-3db6-b69e-7ab4307f5ed0.jpg"},{"name":{"ru":"Pina Colada"},"price":"2 590","id":"4c070584-80a8-3428-9233-13e222241ffe","desc":{"ru":"ТРАДИЦИОННЫЙ КАРИБСКИЙ АЛКОГОЛЬНЫЙ КОКТЕЙЛЬ, НА ОСНОВЕ СВЕТЛОГО РОМА С КОКОСОВЫМ СИРОПОМ И АНАНАСОВЫМ СОКОМ","kk":"Кокос шәрбаты мен ананас шырыны қосылған ашық ром негізіндегі дәстүрлі Кариб алкогольді коктейлі"},"img":"/menu-img/4c070584-80a8-3428-9233-13e222241ffe-t.jpg","imgFull":"/menu-img/4c070584-80a8-3428-9233-13e222241ffe.jpg"},{"name":{"ru":"Sex on the Beach"},"price":"2 590","id":"3a5a3409-ce14-30bb-bb71-20f553458777","desc":{"ru":"АЛКОГОЛЬНЫЙ КОКТЕЙЛЬ, СОДЕРЖАЩИЙ ВОДКУ, ПЕРСИКОВЫЙ ШНАПС, АПЕЛЬСИНОВЫЙ И КЛЮКВЕННЫЙ СОК","kk":"Арақ, шабдалы шнапсы, апельсин және мүкжидек шырыны қосылған алкогольді коктейль"},"img":"/menu-img/3a5a3409-ce14-30bb-bb71-20f553458777-t.jpg","imgFull":"/menu-img/3a5a3409-ce14-30bb-bb71-20f553458777.jpg"},{"name":{"ru":"Tequila Sunrise"},"price":"2 590","id":"960f50b9-532e-39c9-98e0-90e8fe7d1486","desc":{"ru":"КРАСИВЫЙ КОКТЕЙЛЬ НА ОСНОВЕ ТЕКИЛЫ, А ГРЕНАДИН, ОСЕДАЯ НА ДНО СТАКАНА СКВОЗЬ АПЕЛЬСИНОВЫЙ СОК, СОЗДАЕТ ЭФФЕКТ, НАПОМИНАЮЩИЙ РАССВЕТ","kk":"Текила негізіндегі әдемі коктейль: гренадин апельсин шырыны арқылы стаканның түбіне шөгіп, таң атқандай әсер береді"},"img":"/menu-img/960f50b9-532e-39c9-98e0-90e8fe7d1486-t.jpg","imgFull":"/menu-img/960f50b9-532e-39c9-98e0-90e8fe7d1486.jpg"},{"name":{"ru":"Whiskey Sour"},"price":"2 790","id":"b25f56b9-135f-35c4-b27d-8c8e10d6bd42","desc":{"ru":"«ВИСКИ САУЭР» - СМЕШАННЫЙ НАПИТОК, СОДЕРЖАЩИЙ БУРБОН, ЛИМОННЫЙ СОК И САХАР, С ДОБАВЛЕНИЕМ НЕБОЛЬШОГО КОЛИЧЕСТВА ЯИЧНОГО БЕЛКА","kk":"«Виски сауэр»: бурбон, лимон шырыны, қант және аздаған жұмыртқа ағы қосылған аралас сусын"},"img":"/menu-img/b25f56b9-135f-35c4-b27d-8c8e10d6bd42-t.jpg","imgFull":"/menu-img/b25f56b9-135f-35c4-b27d-8c8e10d6bd42.jpg"},{"name":{"ru":"New York Sour"},"price":"2 790","id":"5748dfc9-a7c0-3d2b-a3a7-11699862d430","desc":{"ru":"АЛКОГОЛЬНЫЙ КОКТЕЙЛЬ КРЕПОСТЬЮ 18-20% ОБ. С НАСЫЩЕННЫМ ФРУКТОВЫМ ВКУСОМ, ЛЕГКОЙ КИСЛИНКОЙ И НОТКАМИ ТЕРПКОСТИ В ПОСЛЕВКУСИИ","kk":"Күштілігі 18–20% алкогольді коктейль: дәмі бай жемісті, жеңіл қышқылдығы және соңғы дәмінде қойыртпақ реңктері бар"},"img":"/menu-img/5748dfc9-a7c0-3d2b-a3a7-11699862d430-t.jpg","imgFull":"/menu-img/5748dfc9-a7c0-3d2b-a3a7-11699862d430.jpg"},{"name":{"ru":"Mai Tai"},"price":"2 790","id":"c2b75ab7-7202-361f-a25f-f804e8d0db5d","img":"/menu-img/c2b75ab7-7202-361f-a25f-f804e8d0db5d-t.jpg","imgFull":"/menu-img/c2b75ab7-7202-361f-a25f-f804e8d0db5d.jpg"},{"name":{"ru":"Negroni"},"price":"2 790","id":"33f4ebb9-9286-3093-9176-c3f0c8f431b5","desc":{"ru":"АЛКОГОЛЬНЫЙ КОКТЕЙЛЬ-АПЕРИТИВ НА ОСНОВЕ ДЖИНА И ВЕРМУТА","kk":"Джин мен вермут негізіндегі алкогольді аперитив-коктейль"},"img":"/menu-img/33f4ebb9-9286-3093-9176-c3f0c8f431b5-t.jpg","imgFull":"/menu-img/33f4ebb9-9286-3093-9176-c3f0c8f431b5.jpg"},{"name":{"ru":"Bacardi Mojito"},"price":"2 790","id":"55043d6a-50c0-38c9-bd21-403875e0159f","desc":{"ru":"КОКТЕЙЛЬ НА ОСНОВЕ СВЕТЛОГО РОМА И ЛИСТЬЕВ МЯТЫ. ПРОИСХОДИТ С ОСТРОВА КУБА, СТАЛ ПОПУЛЯРЕН В США В 1980-Х","kk":"Ашық ром мен жалбыз жапырақтары негізіндегі коктейль. Куба аралынан шыққан, АҚШ-та 1980 жылдары танымал болды"},"img":"/menu-img/55043d6a-50c0-38c9-bd21-403875e0159f-t.jpg","imgFull":"/menu-img/55043d6a-50c0-38c9-bd21-403875e0159f.jpg"},{"name":{"ru":"Aperol Spritz"},"price":"3 290","id":"43168f64-9447-3396-8724-3d9605c8a2ef","desc":{"ru":"СЛАБОАЛКОГОЛЬНЫЙ КОКТЕЙЛЬ, ПРЕДСТАВЛЯЮЩИЙ СОБОЙ СМЕСЬ ИГРИСТОГО ВИНА, БИТТЕРА APEROL И СОДОВОЙ ВОДЫ","kk":"Шампан шарабы, Aperol биттері және сода суы қосылған әлсіз алкогольді коктейль"},"img":"/menu-img/43168f64-9447-3396-8724-3d9605c8a2ef-t.jpg","imgFull":"/menu-img/43168f64-9447-3396-8724-3d9605c8a2ef.jpg"},{"name":{"ru":"Long Island Icead Tea"},"price":"2 990","id":"a976330d-df1e-35d4-8dcc-925f5c6f3b18","desc":{"ru":"ОДИН ИЗ САМЫХ КРЕПКИХ КОКТЕЙЛЕЙ, НА ОСНОВЕ ВОДКИ, ДЖИНА, ТЕКИЛЫ И РОМА, С ДОБАВЛЕНИЕМ ЦИТРУСОВОГО ЛИКЕРА И КОКА КОЛЫ","kk":"Арақ, джин, текила және ром негізінде, цитрус ликері мен кока-кола қосылған ең күшті коктейльдердің бірі"},"img":"/menu-img/a976330d-df1e-35d4-8dcc-925f5c6f3b18-t.jpg","imgFull":"/menu-img/a976330d-df1e-35d4-8dcc-925f5c6f3b18.jpg"},{"name":{"ru":"Clover Club"},"price":"2 790","id":"9424f59c-2328-3fb0-a65a-82ddb479eb88","desc":{"ru":"КОКТЕЙЛЬ НА ОСНОВЕ ДЖИНА, ЛИМОННОГО СОКА, МАЛИНОВОГО СИРОПА И ЯИЧНОГО БЕЛКА. ЯИЧНЫЙ БЕЛОК ДОБАВЛЯЕТСЯ РАДИ ОБРАЗОВАНИЯ ХАРАКТЕРНОЙ ПЕНЫ","kk":"Джин, лимон шырыны, таңқурай шәрбаты және жұмыртқа ағы негізіндегі коктейль. Жұмыртқа ағы ерекше көбік үшін қосылады"},"img":"/menu-img/9424f59c-2328-3fb0-a65a-82ddb479eb88-t.jpg","imgFull":"/menu-img/9424f59c-2328-3fb0-a65a-82ddb479eb88.jpg"},{"name":{"ru":"Margarita"},"price":"2 690","id":"5394909e-c350-3c10-8b1a-bd77aaacce73","desc":{"ru":"КОКТЕЙЛЬ НА ОСНОВЕ ТЕКИЛЫ С ЛИКЕРОМ И СОКОМ ЛАЙМА","kk":"Ликер мен лайм шырыны қосылған текила негізіндегі коктейль"},"img":"/menu-img/5394909e-c350-3c10-8b1a-bd77aaacce73-t.jpg","imgFull":"/menu-img/5394909e-c350-3c10-8b1a-bd77aaacce73.jpg"},{"name":{"ru":"Martini Fiero Tonic"},"price":"2 690","id":"0972cab5-8a52-32c6-8f5b-e6463273ba80","desc":{"ru":"ЛЕДЯНОЙ ТОНИК СМЯГЧАЕТ СЛАДКИЕ ВАНИЛЬНЫЕ НОТКИ ВЕРМУТА, ДАРЯ ЯРКОЕ ПОСЛЕВКУСИЕ","kk":"Мұздай тоник вермуттың тәтті ваниль реңкін жұмсартып, жарқын дәм қалдырады"},"img":"/menu-img/0972cab5-8a52-32c6-8f5b-e6463273ba80-t.jpg","imgFull":"/menu-img/0972cab5-8a52-32c6-8f5b-e6463273ba80.jpg"},{"name":{"ru":"French 75"},"price":"2 790","id":"dea6b464-5927-35e2-975f-496ac6efb677","desc":{"ru":"КОКТЕЙЛЬ НА ОСНОВЕ ДЖИНА, ШАМПАНСКОГО И ЛИМОННОГО СОКА","kk":"Джин, шампан шарабы және лимон шырыны негізіндегі коктейль"}},{"name":{"ru":"Arno"},"price":"2 790","id":"07d9dd3e-81e2-34da-b7f5-a99deea6f326","desc":{"ru":"КОКТЕЙЛЬ НА ОСНОВЕ ДЖИНА, СУХОГО ВЕРМУРТА И ПЕРСИКОВОГО ЛИКЕРА, АЛКОГОЛЬНЫЙ И КРЕПКИЙ","kk":"Джин, құрғақ вермут және шабдалы ликері негізіндегі күшті алкогольді коктейль"},"img":"/menu-img/07d9dd3e-81e2-34da-b7f5-a99deea6f326-t.jpg","imgFull":"/menu-img/07d9dd3e-81e2-34da-b7f5-a99deea6f326.jpg"},{"name":{"ru":"B-52"},"price":"1 500","id":"242bb5cd-5575-353f-a4f1-7c3e3af9e0d9","desc":{"ru":"СЛОИСТЫЙ КОКТЕЙЛЬ ИЗ ТРЁХ ЛИКЁРОВ: КОФЕЙНЫЙ, СЛИВОЧНЫЙ И АПЕЛЬСИНОВЫЙ","kk":"Үш ликерден жасалған қабатты коктейль: кофе, кілегей және апельсин ликерлері"},"img":"/menu-img/242bb5cd-5575-353f-a4f1-7c3e3af9e0d9-t.jpg","imgFull":"/menu-img/242bb5cd-5575-353f-a4f1-7c3e3af9e0d9.jpg"},{"name":{"ru":"B-53"},"price":"1 500","id":"46dc75ac-6486-31f8-88a0-1e778c9953ba","desc":{"ru":"СЛОИСТЫЙ КОКТЕЙЛЬ ИЗ КОФЕЙНОГО ЛИКЁРА, СЛИВОЧНОГО ЛИКЁРА И ЗЕЛЕНОГО АБСЕНТА","kk":"Кофе ликері, кілегей ликері және жасыл абсенттен жасалған қабатты коктейль"},"img":"/menu-img/46dc75ac-6486-31f8-88a0-1e778c9953ba-t.jpg","imgFull":"/menu-img/46dc75ac-6486-31f8-88a0-1e778c9953ba.jpg"},{"name":{"ru":"Mojito Pink"},"price":"1 590","id":"cb67f3ae-0f6e-31d5-ab0f-5e681110071a","desc":{"ru":"БЕЗАЛКОГОЛЬНЫЙ КОКТЕЙЛЬ С ЛАЙМОМ С НОТКАМИ КЛУБНИКИ И НЕЖНОЙ МЯТЫ","kk":"Құлпынай реңктері мен нәзік жалбыз қосылған лаймды алкогольсіз коктейль"}},{"name":{"ru":"Mojito"},"price":"1 590","id":"76489c85-caef-3abe-b323-992aa6e812a6","desc":{"ru":"БЕЗАЛКОГОЛЬНЫЙ ОСВЕЖАЮЩИЙ КОКТЕЙЛЬ С ЛАЙМОМ И МЯТОЙ","kk":"Лайм мен жалбыз қосылған сергітетін алкогольсіз коктейль"},"img":"/menu-img/76489c85-caef-3abe-b323-992aa6e812a6-t.jpg","imgFull":"/menu-img/76489c85-caef-3abe-b323-992aa6e812a6.jpg"},{"name":{"ru":"Sunrise"},"price":"1 590","id":"199a8847-5790-3e8c-9a19-4082ff610398","desc":{"ru":"БЕЗАЛКОГОЛЬНЫЙ КОКТЕЙЛЬ НА ОСНОВЕ АПЕЛЬСИНОВОГО СОКА И СИРОПА САНРАЙЗ","kk":"Апельсин шырыны мен «Санрайз» шәрбаты негізіндегі алкогольсіз коктейль"}},{"name":{"ru":"Pina Colada (Cocktail Menu)"},"price":"1 590","id":"5396c652-c3da-3c8a-8f82-7e2b4b9c0ef3","desc":{"ru":"БЕЗАЛКОГОЛЬНЫЙ КОКТЕЙЛЬ НА ОСНОВЕ АНАНАСОВОГО СОКА И КОКОСОВОГО МОЛОКА","kk":"Ананас шырыны мен кокос сүті негізіндегі алкогольсіз коктейль"},"img":"/menu-img/5396c652-c3da-3c8a-8f82-7e2b4b9c0ef3-t.jpg","imgFull":"/menu-img/5396c652-c3da-3c8a-8f82-7e2b4b9c0ef3.jpg"},{"name":{"ru":"Gin 97"},"price":"2 790","id":"7e644d45-e215-49ad-a7b5-14d838753bee","img":"/menu-img/7e644d45-e215-49ad-a7b5-14d838753bee-t.jpg","imgFull":"/menu-img/7e644d45-e215-49ad-a7b5-14d838753bee.jpg"},{"name":{"ru":"Gin bull"},"price":"2 890","id":"727680ee-819e-41e4-a797-8e7b53c1f675","img":"/menu-img/727680ee-819e-41e4-a797-8e7b53c1f675-t.jpg","imgFull":"/menu-img/727680ee-819e-41e4-a797-8e7b53c1f675.jpg"}],"id":"64068a96-0576-3e41-a834-821838ddfbac"}]};

// Per-venue identity. The key is the ?venue=<slug> the frontend sends ("main"
// = GO pub, which never sends one). name shows up in push notifications;
// menu/unavailable seed a brand-new venue's board exactly once.
const VENUES = {
  main: { name: "GO pub", menu: DEFAULT_MENU, unavailable: [] },
  "garage-music-bar": { name: "Garage Music Bar", menu: GARAGE_MENU, unavailable: ["kitchen|Рыбное ассорти на компанию + белое Вино Алазанская Долина", "kitchen|Вино Цинандали +винная тарелка", "kitchen|Окрошка", "bar|Paulaner 0,5 ml (Германия)", "bar|Paulaner 0,5 ml", "bar|Jameson Black Barrel 0,05 ml", "bar|Sanama Reserva Sauvignon Blanc (бел.сух)"] },
};


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
const ROLE_LABELS_LOG = { waiter: "Официант", cook: "Повар", bartender: "Бармен", manager: "Менеджер" };

// Every action logged via log() is bucketed into one of these categories,
// which is what the monitor role's notification checkboxes actually
// control — unmapped/future action types default to "other" so nothing
// is silently invisible to a monitor watching "other".
const ACTION_CATEGORY = {
  new_order: "orders", kitchen_accept: "orders", bar_accept: "orders", kitchen_ready: "orders",
  bar_ready: "orders", served: "orders", close_table: "orders",
  call_waiter: "calls", guest_quick_request: "calls", guest_repeat_request: "calls", request_bill: "calls",
  request_cancel_order: "cancels", approve_cancel: "cancels", reject_cancel: "cancels",
  confirm_repeat_request: "cancels", reject_repeat_request: "cancels",
  create_staff: "staff", delete_staff: "staff", reassign_staff: "staff", promote_master: "staff",
  set_name: "staff", set_staff_pin: "staff", change_pin: "staff",
  add_menu_category: "menu", rename_menu_category: "menu", delete_menu_category: "menu",
  add_menu_item: "menu", update_menu_item: "menu", delete_menu_item: "menu",
  upload_menu_image: "menu", remove_menu_image: "menu",
  disable_items: "menu", enable_items: "menu", set_stock: "menu", clear_stock: "menu",
  create_room: "rooms", update_room: "rooms", delete_room: "rooms", assign_qr: "rooms", unassign_qr: "rooms",
  set_wifi: "other",
};
const NOTIFY_CATEGORIES = ["orders", "calls", "cancels", "staff", "menu", "rooms", "other"];
function defaultNotifyPrefs() {
  const p = {};
  NOTIFY_CATEGORIES.forEach(c => { p[c] = true; });
  return p;
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

async function hmacHex(text, secret) {
  const key = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(text));
  return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 20); // 80-bit truncation — plenty for this, keeps the QR payload short
}

async function signQrId(secret) {
  // A fresh table QR is "<random uuid>.<hmac of that uuid>" — the signature
  // can only be produced by someone holding QR_SIGNING_SECRET (us), so a QR
  // printed/generated anywhere else can never pass verifyQrId below, even if
  // its uuid portion happens to collide or look plausible.
  const uuid = crypto.randomUUID();
  const sig = await hmacHex(uuid, secret);
  return `${uuid}.${sig}`;
}

async function verifyQrId(qrId, secret) {
  if (!qrId || typeof qrId !== "string") return false;
  const dot = qrId.lastIndexOf(".");
  if (dot < 1) return false;
  const uuid = qrId.slice(0, dot);
  const sig = qrId.slice(dot + 1);
  const expected = await hmacHex(uuid, secret);
  return sig === expected;
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
      TTL: "86400", // was 60s — far too short for a screen-off phone; the push service (APNs/FCM) was dropping the notification entirely if it couldn't deliver within that window instead of holding and retrying. 24h keeps it queued for delivery whenever the device becomes reachable, while Urgency below still asks for immediate delivery when possible
      Urgency: "high", // ask the push service (FCM on Android) to attempt immediate delivery, waking the device from Doze mode rather than deferring until the screen turns on
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
    this.wifi = { enabled: false, ssid: "", password: "" }; // guest wifi, shown as a button on the menu when enabled
    this.pins = { waiter: "1111", cook: "1111", bartender: "1111", manager: "1111" };
    this.inventory = { kitchen: {}, bar: {} }; // dest -> { "Name": {qty, threshold} }
    this.pushSubs = []; // [{id, role, staffId, subscription}]
    this.actionLog = []; // [{ts, role, staffName, action, details}]
    this.menu = JSON.parse(JSON.stringify(DEFAULT_MENU)); // own copy — never mutate the shared default. { kitchen: [...], bar: [...] } — categories/items carry stable ids
    this.qrAssignments = {}; // qrId -> global table number, set by the manager after scanning a printed QR
    this.cancelRequests = []; // [{id, orderId, table, requestedBy, requestedAt}] — pending manager approval
    this.pendingEscalations = []; // [{orderId, table, dueAt}] — "still not served after 2 min" checks, tracked via the DO alarm
    this.pendingBillEscalations = []; // [{table, dueAt}] — "still no reaction to a bill request after 2 min"
    this.callingTables = {}; // table -> true, while a guest has called for a waiter and no waiter has acknowledged it yet
    this.billRequestedTables = {}; // table -> true, while a guest has asked for the bill and no waiter has acknowledged it yet
    this.returningGuestTables = {}; // table -> true, while a recognized repeat guest is present and no waiter has acknowledged it yet
    this.repeatRequests = []; // [{id, table, items, requestedAt}] — guest-initiated "repeat my order" asks, pending a waiter's confirmation before becoming a real order
    this.guestHistory = {}; // deviceId -> [{items:[{name,qty}], ts}] — only for guests who explicitly opted in
    this.tableGuestDevice = {}; // table -> deviceId, only set for devices with prior consent-based history
    this.venue = null; // slug this DO serves, learned from the Worker's X-Venue header and persisted
    this.ready = this.state.blockConcurrencyWhile(async () => {
      const storedOrders = await this.state.storage.get("orders");
      const storedHistory = await this.state.storage.get("history");
      const storedOpen = await this.state.storage.get("openTables");
      const storedClosed = await this.state.storage.get("closedTables");
      const storedStaff = await this.state.storage.get("staff");
      const storedRooms = await this.state.storage.get("rooms");
      const storedTableCount = await this.state.storage.get("tableCount"); // legacy, pre-rooms
      const storedUnavailable = await this.state.storage.get("unavailable");
      const storedWifi = await this.state.storage.get("wifi");
      const storedPins = await this.state.storage.get("pins");
      const storedInventory = await this.state.storage.get("inventory");
      const storedPushSubs = await this.state.storage.get("pushSubs");
      const storedActionLog = await this.state.storage.get("actionLog");
      const storedMenu = await this.state.storage.get("menu");
      const storedQrAssignments = await this.state.storage.get("qrAssignments");
      const storedCancelRequests = await this.state.storage.get("cancelRequests");
      const storedPendingEscalations = await this.state.storage.get("pendingEscalations");
      const storedPendingBillEscalations = await this.state.storage.get("pendingBillEscalations");
      const storedCallingTables = await this.state.storage.get("callingTables");
      const storedBillRequestedTables = await this.state.storage.get("billRequestedTables");
      const storedReturningGuestTables = await this.state.storage.get("returningGuestTables");
      const storedRepeatRequests = await this.state.storage.get("repeatRequests");
      const storedGuestHistory = await this.state.storage.get("guestHistory");
      const storedTableGuestDevice = await this.state.storage.get("tableGuestDevice");
      const storedVenue = await this.state.storage.get("venue");
      if (storedVenue) this.venue = storedVenue;
      if (storedOrders) this.orders = storedOrders;
      if (storedHistory) this.history = storedHistory;
      if (storedOpen) this.openTables = storedOpen;
      if (storedClosed) this.closedTables = storedClosed;
      if (storedStaff) this.staff = storedStaff;
      if (storedRooms) this.rooms = storedRooms;
      else if (storedTableCount) this.rooms = [{ id: "default", name: "Зал", tableCount: storedTableCount }];
      if (storedUnavailable) this.unavailable = storedUnavailable;
      if (storedWifi) this.wifi = storedWifi;
      if (storedPins) this.pins = storedPins;
      if (storedInventory) this.inventory = storedInventory;
      if (storedPushSubs) this.pushSubs = storedPushSubs;
      if (storedActionLog) this.actionLog = storedActionLog;
      if (storedMenu) this.menu = storedMenu;
      if (storedQrAssignments) this.qrAssignments = storedQrAssignments;
      if (storedCancelRequests) this.cancelRequests = storedCancelRequests;
      if (storedPendingEscalations) this.pendingEscalations = storedPendingEscalations;
      if (storedPendingBillEscalations) this.pendingBillEscalations = storedPendingBillEscalations;
      if (storedCallingTables) this.callingTables = storedCallingTables;
      if (storedBillRequestedTables) this.billRequestedTables = storedBillRequestedTables;
      if (storedReturningGuestTables) this.returningGuestTables = storedReturningGuestTables;
      if (storedRepeatRequests) this.repeatRequests = storedRepeatRequests;
      if (storedGuestHistory) this.guestHistory = storedGuestHistory;
      if (storedTableGuestDevice) this.tableGuestDevice = storedTableGuestDevice;

      if (!this.staff.some(s => s.role === "manager")) {
        // First run after switching managers over to personal accounts —
        // carry the old shared PIN forward as the master manager's starting
        // PIN, so whoever knew it before can still get in and rename
        // themselves via the usual "Изменить имя" flow. Fixed id (not
        // randomUUID) so RestoRUN's hardcoded handoff always matches.
        this.staff.push({
          id: "a0b1ae81-06fe-4e45-b95f-e7c3c2929a10",
          role: "manager",
          name: "Мастер-менеджер",
          pin: this.pins.manager || "1111",
          isMaster: true,
        });
        await this.persist();
      }

      if (!this.staff.some(s => s.role === "monitor")) {
        // The system creator's own read-only watch role. Deliberately never
        // created through create_staff, never shown in manager.html's staff
        // list, and filtered out of every non-monitor client's state/action
        // log (see stateSnapshot). Fixed id so the one private invite link
        // keeps working across deploys; not logged, so its bootstrap leaves
        // no trace in the action feed either.
        this.staff.push({
          id: "f3d8c9a2-5b41-4e7a-9c02-6a8e21b4d9f7",
          role: "monitor",
          name: "",
          pin: "1111",
          notifyPrefs: defaultNotifyPrefs(),
        });
        await this.persist();
      }
    });
  }

  get brand() {
    return (VENUES[this.venue || "main"] || VENUES.main).name;
  }

  // First contact with a venue slug: remember it, and if this is a brand-new
  // non-GO venue whose menu is still the untouched GO pub default (the
  // constructor seeds that before it knows which venue it is), swap in the
  // venue's own starting menu. GO pub ("main") and any already-edited menu
  // are never touched.
  async adoptVenue(slug) {
    if (!slug || this.venue === slug) return;
    if (this.venue) return; // a DO serves exactly one venue for life
    this.venue = slug;
    const cfg = VENUES[slug];
    if (slug !== "main" && cfg && JSON.stringify(this.menu) === JSON.stringify(DEFAULT_MENU)) {
      this.menu = JSON.parse(JSON.stringify(cfg.menu));
      this.unavailable = {};
      (cfg.unavailable || []).forEach(key => {
        this.unavailable[key] = { comment: "Нет в наличии", disabledBy: null, disabledAt: Date.now() };
      });
      await this.persist();
    }
    await this.state.storage.put("venue", slug);
  }

  allMenuItems() {
    const out = [];
    ["kitchen", "bar"].forEach(dest => (this.menu[dest] || []).forEach(cat =>
      (cat.items || []).forEach(item => out.push({ dest, cat, item }))));
    return out;
  }

  findMenuItem(dest, name) {
    for (const cat of this.menu[dest] || []) {
      const item = (cat.items || []).find(i => i.name.ru === name);
      if (item) return item;
    }
    return null;
  }

  // A combo is unorderable when any of its parts is itself a menu item that
  // the other station has switched off (e.g. the bar ran out of the beer).
  comboBlockedBy(dest, name) {
    const item = this.findMenuItem(dest, name);
    for (const p of (item && item.parts) || []) {
      if (this.unavailable[p.dest + "|" + p.name]) return p;
    }
    return null;
  }

  isOrderable(dest, name) {
    return !this.unavailable[dest + "|" + name] && !this.comboBlockedBy(dest, name);
  }

  // What clients see as "unavailable": the stored switches plus combos that
  // are blocked by a missing part. Derived entries are flagged so they are
  // never written back to storage.
  effectiveUnavailable() {
    const out = { ...this.unavailable };
    this.allMenuItems().forEach(({ dest, item }) => {
      const key = dest + "|" + item.name.ru;
      if (out[key]) return;
      const p = this.comboBlockedBy(dest, item.name.ru);
      if (p) out[key] = { comment: `Нет компонента: ${p.name}`, disabledBy: null, disabledAt: null, derived: true };
    });
    return out;
  }

  // Splits combos: every billed line whose menu item has parts gets those
  // parts added (qty-multiplied, price 0, flagged component) to the station
  // that actually prepares them. Client-sent component flags are ignored.
  expandCombos(kitchenItems, barItems) {
    const lists = {
      kitchen: (kitchenItems || []).filter(i => !i.component),
      bar: (barItems || []).filter(i => !i.component),
    };
    const extra = { kitchen: [], bar: [] };
    ["kitchen", "bar"].forEach(dest => {
      lists[dest].forEach(line => {
        const item = this.findMenuItem(dest, line.name);
        ((item && item.parts) || []).forEach(p => {
          extra[p.dest].push({
            name: p.name,
            qty: (p.qty || 1) * (line.qty || 1),
            price: "0",
            note: `комбо «${line.name}»` + (p.note ? `, ${p.note}` : ""),
            component: true,
            comboOf: line.name,
          });
        });
      });
    });
    return { kitchenItems: [...lists.kitchen, ...extra.kitchen], barItems: [...lists.bar, ...extra.bar] };
  }

  // Photos uploaded from manager.html are kept in this DO's own storage
  // (key "img:<venue>/<uuid>.jpg") and served by the Worker's /img/ route.
  // Static photos (/menu-img/… in the venue's Pages repo) are only unlinked.
  imgKeyIsOurs(key) {
    return typeof key === "string" && IMG_KEY_RE.test(key) && key.startsWith((this.venue || "main") + "/");
  }

  async deleteItemImages(item) {
    const keys = [item.img, item.imgFull].filter(k => this.imgKeyIsOurs(k)).map(k => "img:" + k);
    if (keys.length) await this.state.storage.delete(keys);
    delete item.img; delete item.imgFull;
  }

  async putImage(bytes, type, ext) {
    const key = `${this.venue || "main"}/${crypto.randomUUID()}.${ext}`;
    await this.state.storage.put("img:" + key, { bytes, type });
    return key;
  }

  async serveImage(key) {
    const rec = IMG_KEY_RE.test(key) ? await this.state.storage.get("img:" + key) : null;
    if (!rec) return new Response("Not found", { status: 404, headers: { "Access-Control-Allow-Origin": "*" } });
    return new Response(rec.bytes, { headers: {
      "Content-Type": rec.type,
      "Cache-Control": "public, max-age=31536000, immutable",
      "Access-Control-Allow-Origin": "*",
    } });
  }

  // One-time upgrade of a venue menu that was seeded by an older build of
  // this Worker: copies photo paths and combo parts from the venue's
  // starting menu onto the stored items (matched by item id). Never
  // overwrites a photo or combo the manager has set, and runs once per DO.
  async migrateVenueMenu() {
    const MIGRATION = "menu-photos-combos-v1";
    const cfg = VENUES[this.venue || ""];
    if (!this.venue || this.venue === "main" || !cfg) return;
    const done = (await this.state.storage.get("migrations")) || [];
    if (done.includes(MIGRATION)) return;
    const source = {};
    ["kitchen", "bar"].forEach(dest => (cfg.menu[dest] || []).forEach(cat =>
      (cat.items || []).forEach(it => { source[it.id] = it; })));
    let changed = 0;
    this.allMenuItems().forEach(({ item }) => {
      const src = source[item.id];
      if (item.importImg) { delete item.importImg; changed++; }
      if (!src) return;
      if (src.img && !item.img && !item.imgFull) { item.img = src.img; item.imgFull = src.imgFull; changed++; }
      if (src.parts && !item.parts) { item.parts = JSON.parse(JSON.stringify(src.parts)); changed++; }
    });
    if (changed) await this.persist();
    await this.state.storage.put("migrations", [...done, MIGRATION]);
    if (changed) { console.log(`[migrate] ${this.venue}: ${changed} menu fields updated`); this.broadcast(); }
  }

  // One-time: brings Kazakh names/descriptions/section titles of an already
  // seeded venue menu up to the venue's starting menu (fixes wrong or missing
  // kk text). Only touches entries whose Russian text the manager hasn't
  // changed, so nothing the manager edited is overwritten.
  async migrateVenueTranslations() {
    const MIGRATION = "menu-kk-v1";
    const cfg = VENUES[this.venue || ""];
    if (!this.venue || this.venue === "main" || !cfg) return;
    const done = (await this.state.storage.get("migrations")) || [];
    if (done.includes(MIGRATION)) return;
    const srcItems = {}, srcCats = {};
    ["kitchen", "bar"].forEach(dest => (cfg.menu[dest] || []).forEach(cat => {
      srcCats[cat.id] = cat;
      (cat.items || []).forEach(it => { srcItems[it.id] = it; });
    }));
    let changed = 0;
    const sync = (dst, src) => {
      if (!dst || !src || dst.ru !== src.ru || !src.kk || dst.kk === src.kk) return;
      dst.kk = src.kk; changed++;
    };
    ["kitchen", "bar"].forEach(dest => (this.menu[dest] || []).forEach(cat => {
      const sc = srcCats[cat.id];
      if (sc) { sync(cat.title, sc.title); if (cat.note && sc.note) sync(cat.note, sc.note); }
      (cat.items || []).forEach(item => {
        const si = srcItems[item.id]; if (!si) return;
        sync(item.name, si.name);
        if (item.desc && si.desc) sync(item.desc, si.desc);
      });
    }));
    if (changed) await this.persist();
    await this.state.storage.put("migrations", [...done, MIGRATION]);
    if (changed) { console.log(`[migrate] ${this.venue}: ${changed} Kazakh texts updated`); this.broadcast(); }
  }

  async persist() {
    await this.state.storage.put("orders", this.orders);
    await this.state.storage.put("history", this.history);
    await this.state.storage.put("openTables", this.openTables);
    await this.state.storage.put("closedTables", this.closedTables);
    await this.state.storage.put("staff", this.staff);
    await this.state.storage.put("rooms", this.rooms);
    await this.state.storage.put("unavailable", this.unavailable);
    await this.state.storage.put("wifi", this.wifi);
    await this.state.storage.put("pins", this.pins);
    await this.state.storage.put("inventory", this.inventory);
    await this.state.storage.put("pushSubs", this.pushSubs);
    await this.state.storage.put("actionLog", this.actionLog);
    await this.state.storage.put("menu", this.menu);
    await this.state.storage.put("qrAssignments", this.qrAssignments);
    await this.state.storage.put("cancelRequests", this.cancelRequests);
    await this.state.storage.put("pendingEscalations", this.pendingEscalations);
    await this.state.storage.put("pendingBillEscalations", this.pendingBillEscalations);
    await this.state.storage.put("callingTables", this.callingTables);
    await this.state.storage.put("billRequestedTables", this.billRequestedTables);
    await this.state.storage.put("returningGuestTables", this.returningGuestTables);
    await this.state.storage.put("repeatRequests", this.repeatRequests);
    await this.state.storage.put("guestHistory", this.guestHistory);
    await this.state.storage.put("tableGuestDevice", this.tableGuestDevice);
  }

  log(conn, action, details) {
    const entry = {
      ts: Date.now(),
      role: conn.role || null,
      staffName: conn.staffName || null,
      action,
      details: details || {},
    };
    this.actionLog.push(entry);
    if (this.actionLog.length > 1000) this.actionLog = this.actionLog.slice(-1000);
    this.notifyMonitors(entry);
  }

  // Human-readable one-liner for a logged action — used both for the
  // monitor role's live feed and its push notification body. Mirrors
  // manager.html's own actionText() so the wording matches everywhere.
  actionText(e) {
    const d = e.details || {};
    const who = e.staffName || ROLE_LABELS_LOG[e.role] || "Кто-то";
    const t = (n) => this.tableLabel(n);
    switch (e.action) {
      case "new_order": return `${who} создал заказ — стол ${t(d.table)} (кухня: ${d.kitchenCount}, бар: ${d.barCount})`;
      case "kitchen_accept": return `${who} принял в работу — стол ${t(d.table)} (кухня)`;
      case "bar_accept": return `${who} принял в работу — стол ${t(d.table)} (бар)`;
      case "kitchen_ready": return `${who} отметил готово — стол ${t(d.table)} (кухня)`;
      case "bar_ready": return `${who} отметил готово — стол ${t(d.table)} (бар)`;
      case "served": return `${who} выдал заказ гостю — стол ${t(d.table)}`;
      case "close_table": return `${who} закрыл стол ${t(d.table)} — ${(d.total || 0).toLocaleString("ru-RU")} тг`;
      case "disable_items": return `${who} убрал из меню (${d.dest === "kitchen" ? "кухня" : "бар"}): ${(d.names || []).join(", ")}`;
      case "enable_items": return `${who} вернул в меню (${d.dest === "kitchen" ? "кухня" : "бар"}): ${(d.names || []).join(", ")}`;
      case "set_stock": return `${who} обновил склад (${d.dest === "kitchen" ? "кухня" : "бар"}): ${d.name} — остаток ${d.qty}, порог ${d.threshold}`;
      case "clear_stock": return `${who} снял отслеживание склада: ${d.name}`;
      case "create_staff": return `${who} добавил сотрудника: ${ROLE_LABELS_LOG[d.role] || d.role}`;
      case "delete_staff": return `${who} удалил сотрудника: ${d.name || ROLE_LABELS_LOG[d.role] || ""}`;
      case "reassign_staff": return `${who} переназначил устройство сотрудника: ${d.name || ROLE_LABELS_LOG[d.role] || ""}`;
      case "promote_master": return `${who} передал мастер-права: ${d.name || ""}`;
      case "create_room": return `${who} создал зал «${d.name}»`;
      case "update_room": return `${who} изменил зал «${d.name}» (столов: ${d.tableCount})`;
      case "delete_room": return `${who} удалил зал «${d.name || ""}»`;
      case "set_name": return `${who} задал имя: ${d.name}`;
      case "set_staff_pin": return `${who} сменил свой личный PIN`;
      case "change_pin": return `${who} сменил PIN (${ROLE_LABELS_LOG[d.role] || d.role})`;
      case "assign_qr": return `${who} привязал QR к столу ${t(d.table)}`;
      case "unassign_qr": return `${who} отвязал QR-код`;
      case "call_waiter": return `Стол ${t(d.table)} позвал официанта`;
      case "guest_quick_request": return `Стол ${t(d.table)} просит: ${d.what || ""}`;
      case "guest_repeat_request": return `Стол ${t(d.table)} хочет повторить заказ`;
      case "request_bill": return `Стол ${t(d.table)} попросил расчёт`;
      case "request_cancel_order": return `${who} запросил отмену заказа — стол ${t(d.table)}`;
      case "approve_cancel": return `${who} подтвердил отмену заказа — стол ${t(d.table)}`;
      case "reject_cancel": return `${who} отклонил отмену заказа — стол ${t(d.table)}`;
      case "confirm_repeat_request": return `${who} подтвердил повтор заказа — стол ${t(d.table)}`;
      case "reject_repeat_request": return `${who} отклонил повтор заказа`;
      case "add_menu_category": return `${who} добавил категорию меню: ${d.title || ""}`;
      case "rename_menu_category": return `${who} переименовал категорию меню: ${d.title || ""}`;
      case "delete_menu_category": return `${who} удалил категорию меню: ${d.title || ""}`;
      case "add_menu_item": return `${who} добавил в меню: ${d.name || ""}`;
      case "update_menu_item": return `${who} изменил позицию меню: ${d.name || ""}`;
      case "upload_menu_image": return `${who} загрузил фото: ${d.name || ""}`;
      case "remove_menu_image": return `${who} убрал фото: ${d.name || ""}`;
      case "delete_menu_item": return `${who} удалил из меню: ${d.name || ""}`;
      case "set_wifi": return `${who} ${d.enabled ? "включил" : "выключил"} Wi-Fi для гостей`;
      default: return `${who}: ${e.action}`;
    }
  }

  // Pushes a notification to every "monitor" staff member whose saved
  // preferences include this action's category. Monitors are read-only —
  // this is the only thing that ever reaches out to them.
  notifyMonitors(entry) {
    const category = ACTION_CATEGORY[entry.action] || "other";
    const monitors = this.staff.filter(s => s.role === "monitor" && (!s.notifyPrefs || s.notifyPrefs[category] !== false));
    if (monitors.length === 0) return;
    const body = this.actionText(entry);
    const text = { title: this.brand, body, url: "/monitor.html" };
    monitors.forEach(m => {
      const subs = this.pushSubs.filter(p => p.staffId === m.id);
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
    });
  }

  stateSnapshot(opts) {
    const hideMonitor = !opts || opts.hideMonitor !== false; // default true — only a monitor connection ever passes hideMonitor:false
    return {
      type: "state",
      orders: this.orders,
      history: this.history,
      openTables: this.openTables,
      closedTables: this.closedTables,
      staff: hideMonitor ? this.staff.filter(s => s.role !== "monitor") : this.staff,
      rooms: this.rooms,
      tableCount: this.rooms.reduce((s, r) => s + (r.tableCount || 0), 0), // kept for any old client still reading it
      unavailable: this.effectiveUnavailable(),
      inventory: this.inventory,
      actionLog: hideMonitor ? this.actionLog.filter(e => e.role !== "monitor") : this.actionLog,
      menu: this.menu,
      qrAssignments: this.qrAssignments,
      cancelRequests: this.cancelRequests,
      callingTables: this.callingTables,
      billRequestedTables: this.billRequestedTables,
      returningGuestTables: this.returningGuestTables,
      repeatRequests: this.repeatRequests,
      wifi: this.wifi,
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
      unavailable: this.effectiveUnavailable(),
      vapidPublicKey: this.env.VAPID_PUBLIC_KEY || null,
      wifi: this.wifi.enabled ? { ssid: this.wifi.ssid, password: this.wifi.password } : null,
    };
  }

  computeTableBill(table) {
    const session = this.openTables[table];
    if (!session) return { hasOrders: false, items: [], subtotal: 0, service: 0, total: 0, status: null, stations: { kitchen: null, bar: null } };
    const rounds = this.history.filter(h => h.table === table && h.servedAt >= session.openedAt);
    const activeOrders = this.orders.filter(o => o.table === table);
    const merged = {};
    const addItems = (items, dest) => {
      (items || []).forEach(i => {
        const key = i.name;
        if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price, dest };
        merged[key].qty += i.qty;
      });
    };
    rounds.forEach(r => { addItems(billable(r.kitchenItems), "kitchen"); addItems(billable(r.barItems), "bar"); });
    activeOrders.forEach(o => { addItems(billable(o.kitchenItems), "kitchen"); addItems(billable(o.barItems), "bar"); });
    const items = Object.values(merged);
    const subtotal = items.reduce((s, i) => s + parsePrice(i.price) * i.qty, 0);
    const service = Math.round(subtotal * SERVICE_RATE);

    let status = null;
    const stations = { kitchen: null, bar: null };
    if (items.length > 0) {
      if (activeOrders.length === 0) {
        status = "served"; // everything ordered so far has already been served
      } else {
        status = activeOrders.every(o => this.isOrderFullyReady(o)) ? "ready" : "preparing";
        const STAGE_RANK = { pending: 0, accepted: 1, ready: 2 };
        const earliestStage = (getStatus, hasItems) => {
          const relevant = activeOrders.filter(hasItems);
          if (relevant.length === 0) return null; // nothing currently in flight for this station
          let worst = 2;
          relevant.forEach(o => { worst = Math.min(worst, STAGE_RANK[getStatus(o)] ?? 2); });
          return Object.keys(STAGE_RANK).find(k => STAGE_RANK[k] === worst);
        };
        stations.kitchen = earliestStage(o => o.kitchenStatus, o => (o.kitchenItems || []).length > 0);
        stations.bar = earliestStage(o => o.barStatus, o => (o.barItems || []).length > 0);
      }
    }

    return { hasOrders: items.length > 0, items, subtotal, service, total: subtotal + service, status, stations };
  }

  broadcast() {
    const fullPayload = JSON.stringify(this.stateSnapshot());
    const monitorPayload = JSON.stringify(this.stateSnapshot({ hideMonitor: false }));
    const guestPayload = JSON.stringify(this.guestSnapshot());
    for (const client of this.sockets) {
      const payload = client.role === "guest" ? guestPayload : (client.role === "monitor" ? monitorPayload : fullPayload);
      try { client.ws.send(payload); } catch (e) { /* ignore dead sockets */ }
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

  pushText(message, role) {
    const urlByRole = { waiter: "/waiter.html?view=board", cook: "/cook.html", bartender: "/bartender.html", manager: "/manager.html", monitor: "/monitor.html" };
    let url = urlByRole[role] || "/";
    if (role === "waiter" && message.kind === "call_waiter") url = "/waiter.html"; // fresh table calling — jump straight to the new-order screen with this table pre-selected, not the board
    if (role === "waiter" && message.table !== undefined) url += (url.includes("?") ? "&" : "?") + "table=" + encodeURIComponent(message.table);
    if (message.kind === "low_stock") return { title: `${this.brand} — заканчивается`, body: `${message.name} — осталось ${message.qty}`, url };
    if (message.kind === "call_waiter") return { title: `${this.brand} — зовут официанта`, body: `Стол ${this.tableLabel(message.table)}`, url };
    if (message.kind === "quick_request") {
      const labels = { cutlery: "просит приборы", napkins: "просит салфетки", toothpicks: "просит зубочистки", salt_pepper: "просит соль/перец", order_problem: "сообщает о проблеме с заказом" };
      return { title: `${this.brand} — запрос от стола`, body: `Стол ${this.tableLabel(message.table)} ${labels[message.what] || "что-то просит"}`, url };
    }
    if (message.kind === "repeat_request") return { title: `${this.brand} — повторный заказ`, body: `Стол ${this.tableLabel(message.table)} хочет повторить заказ — проверьте и подтвердите`, url };
    if (message.kind === "request_bill") return { title: `${this.brand} — просят счёт`, body: `Стол ${this.tableLabel(message.table)}`, url };
    if (message.kind === "cancel_request") return { title: `${this.brand} — запрос на отмену`, body: `Стол ${this.tableLabel(message.table)} — подтвердите или отклоните`, url };
    if (message.kind === "cancel_approved") return { title: `${this.brand} — отмена подтверждена`, body: `Стол ${this.tableLabel(message.table)}`, url };
    if (message.kind === "cancel_rejected") return { title: `${this.brand} — в отмене отказано`, body: `Стол ${this.tableLabel(message.table)}`, url };
    if (message.kind === "cancel_already_resolved") return { title: this.brand, body: `Стол ${this.tableLabel(message.table)} — заказ уже выдан, отменять нечего`, url };
    if (message.kind === "still_not_served") return { title: `${this.brand} — заказ всё ещё не выдан`, body: `Стол ${this.tableLabel(message.table)} — прошло 2 минуты с готовности`, url };
    if (message.kind === "still_no_bill") return { title: `${this.brand} — гость всё ещё ждёт счёт`, body: `Стол ${this.tableLabel(message.table)} — просил счёт уже 2 минуты`, url };
    if (message.part) return { title: `${this.brand} — готово`, body: `Стол ${this.tableLabel(message.table)} (${message.part === "kitchen" ? "кухня" : "бар"})`, url };
    if (message.table !== undefined) return { title: `${this.brand} — новый заказ`, body: `Стол ${this.tableLabel(message.table)}`, url };
    return { title: this.brand, body: "Новое уведомление", url };
  }

  pushToRole(role, message) {
    const subs = this.pushSubs.filter(p =>
      p.role === role && (!p.staffId || this.staff.some(s => s.id === p.staffId))
    ); // staffId-less (manager) always kept; staff roles must still exist in the current roster
    if (subs.length === 0) { console.log(`[push] pushToRole(${role}): 0 subscriptions registered`); return; }
    console.log(`[push] pushToRole(${role}): sending to ${subs.length} subscription(s)`);
    const text = this.pushText(message, role);
    subs.forEach(p => {
      sendWebPush(p.subscription, text, this.env)
        .then(async (resp) => {
          console.log(`[push] pushToRole(${role}) id=${p.id}: status=${resp ? resp.status : "no-response"}`);
          if (resp && !resp.ok) {
            try { console.log(`[push] response body: ${(await resp.text()).slice(0, 300)}`); } catch (e2) {}
          }
          if (resp && (resp.status === 404 || resp.status === 410)) {
            this.pushSubs = this.pushSubs.filter(x => x.id !== p.id);
            await this.persist();
          }
        })
        .catch((e) => { console.log(`[push] pushToRole(${role}) id=${p.id}: threw ${e && e.message}`); });
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

  tableLabel(globalNum) {
    if (this.rooms.length <= 1) return String(globalNum);
    let offset = 0;
    for (const room of this.rooms) {
      if (globalNum <= offset + room.tableCount) return `${room.name} · ${globalNum - offset}`;
      offset += room.tableCount;
    }
    return String(globalNum);
  }

  isOrderFullyReady(order) {
    const kReady = order.kitchenStatus === "ready" || order.kitchenStatus === "none";
    const bReady = order.barStatus === "ready" || order.barStatus === "none";
    return kReady && bReady;
  }

  notifyGuestAtTable(table, message) {
    const payload = JSON.stringify(message);
    for (const client of this.sockets) {
      if (client.role === "guest" && client.table === table) {
        try { client.ws.send(payload); } catch (e) {}
      }
    }
  }

  notifyStaff(staffId, message) {
    // Same as notify(), but targeted at one specific person's device(s)
    // rather than an entire role — used for "tell the waiter who actually
    // owns this order" before falling back to the whole team.
    if (!staffId) return;
    const payload = JSON.stringify({ type: "notify", ...message });
    for (const client of this.sockets) {
      if (client.staffId === staffId) {
        try { client.ws.send(payload); } catch (e) {}
      }
    }
    const subs = this.pushSubs.filter(p => p.staffId === staffId);
    if (subs.length === 0) { console.log(`[push] notifyStaff(${staffId}): 0 subscriptions registered`); return; }
    console.log(`[push] notifyStaff(${staffId}): sending to ${subs.length} subscription(s)`);
    const text = this.pushText(message, "waiter");
    subs.forEach(p => {
      sendWebPush(p.subscription, text, this.env)
        .then(async (resp) => {
          console.log(`[push] notifyStaff(${staffId}) id=${p.id}: status=${resp ? resp.status : "no-response"}`);
          if (resp && !resp.ok) {
            try { console.log(`[push] response body: ${(await resp.text()).slice(0, 300)}`); } catch (e2) {}
          }
          if (resp && (resp.status === 404 || resp.status === 410)) {
            this.pushSubs = this.pushSubs.filter(x => x.id !== p.id);
            await this.persist();
          }
        })
        .catch((e) => { console.log(`[push] notifyStaff(${staffId}) id=${p.id}: threw ${e && e.message}`); });
    });
  }

  async ensureAlarmScheduled() {
    const dues = [
      ...this.pendingEscalations.map(e => e.dueAt),
      ...this.pendingBillEscalations.map(e => e.dueAt),
    ];
    if (dues.length > 0) await this.state.storage.setAlarm(Math.min(...dues));
  }

  async scheduleEscalation(orderId, table) {
    this.pendingEscalations.push({ orderId, table, dueAt: Date.now() + 2 * 60 * 1000 });
    await this.persist();
    await this.ensureAlarmScheduled();
  }

  async scheduleBillEscalation(table) {
    // one pending watch per table is enough — repeated bill requests
    // shouldn't stack up separate escalation timers
    if (this.pendingBillEscalations.some(e => e.table === table)) return;
    this.pendingBillEscalations.push({ table, dueAt: Date.now() + 2 * 60 * 1000 });
    await this.persist();
    await this.ensureAlarmScheduled();
  }

  async alarm() {
    await this.ready;
    const now = Date.now();
    const due = this.pendingEscalations.filter(e => e.dueAt <= now);
    this.pendingEscalations = this.pendingEscalations.filter(e => e.dueAt > now);
    for (const e of due) {
      const stillWaiting = this.orders.find(o => o.id === e.orderId);
      if (stillWaiting) {
        this.notify("waiter", { kind: "still_not_served", table: e.table, orderId: e.orderId });
        this.notify("manager", { kind: "still_not_served", table: e.table, orderId: e.orderId });
      }
    }
    const dueBills = this.pendingBillEscalations.filter(e => e.dueAt <= now);
    this.pendingBillEscalations = this.pendingBillEscalations.filter(e => e.dueAt > now);
    for (const e of dueBills) {
      const stillOpen = !!this.openTables[e.table]; // resolved once the waiter closes the table
      if (stillOpen) {
        this.notify("waiter", { kind: "still_no_bill", table: e.table });
        this.notify("manager", { kind: "still_no_bill", table: e.table });
      }
    }
    await this.persist();
    await this.ensureAlarmScheduled();
  }

  checkPin(role, pin) {
    const expected = this.pins[role];
    if (!expected) return true; // no PIN configured for this role -> allow
    return String(pin || "") === String(expected);
  }

  // Ties a known, consenting device to a table only at the moment it
  // genuinely calls for service (waiter call, quick request, repeat
  // request) — never from the guest page merely being open or reloading in
  // the background, so a stray idle tab can't resurrect a stale binding.
  recognizeReturningGuest(table, deviceId) {
    if (!deviceId) return;
    this.tableGuestDevice[table] = deviceId;
    if ((this.guestHistory[deviceId] || []).length > 0) {
      this.returningGuestTables[table] = true; // only true repeat visitors light up the table — not first-time consenters
    }
  }

  async fetch(request) {
    await this.ready;
    await this.adoptVenue(request.headers.get("X-Venue"));
    await this.migrateVenueMenu();
    await this.migrateVenueTranslations();
    const url = new URL(request.url);
    if (url.pathname.startsWith("/img/")) return this.serveImage(decodeURIComponent(url.pathname.slice(5)));

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
            conn.table = msg.table || null; // lets us notify this specific guest directly (e.g. when their table is closed)
          } else if (msg.staffId) {
            const staff = this.staff.find(s => s.id === msg.staffId && s.role === msg.role);
            if (!staff) {
              server.send(JSON.stringify({ type: "auth_error", reason: "unknown_staff" }));
              server.close(4001, "unknown staff");
              this.sockets.delete(conn);
              return;
            }
            if (!staff.pin) { staff.pin = "1111"; await this.persist(); } // migrate older records
            if (String(msg.pin || "") !== String(staff.pin)) {
              // Right device, wrong (or missing) personal PIN — this is the
              // "locked, please unlock" case, distinct from unknown_staff:
              // the staffId binding on this device stays intact.
              server.send(JSON.stringify({ type: "auth_error", reason: "wrong_pin" }));
              server.close(4001, "wrong staff pin");
              this.sockets.delete(conn);
              return;
            }
            conn.role = msg.role;
            conn.staffId = staff.id;
            conn.staffName = staff.name || "";
            conn.isMaster = !!staff.isMaster;
            conn.authed = true;
          } else {
            // No role can fall back to a shared PIN anymore — a valid
            // personal QR link (staffId) is required for everyone,
            // managers included. This is what makes deleting/reassigning a
            // staff member (or a manager) actually revoke access.
            server.send(JSON.stringify({ type: "auth_error", reason: "staff_required" }));
            server.close(4001, "staff id required");
            this.sockets.delete(conn);
            return;
          }
          server.send(JSON.stringify({ type: "auth_ok" }));
          server.send(JSON.stringify(conn.role === "guest" ? this.guestSnapshot() : this.stateSnapshot({ hideMonitor: conn.role !== "monitor" })));
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

        if (msg.type === "resolve_qr") {
          // Public — a printed table QR only encodes an anonymous id; this
          // is how the customer menu finds out which table it's sitting at.
          const valid = await verifyQrId(msg.qrId, this.env.QR_SIGNING_SECRET);
          this.sendTo(conn, { type: "qr_table", qrId: msg.qrId, table: valid ? (this.qrAssignments[msg.qrId] ?? null) : null });
          return;
        }

        if (!conn.authed) return; // ignore everything until hello succeeds

        if (msg.type === "get_table_bill" && msg.table) {
          this.sendTo(conn, { type: "table_bill", table: msg.table, calling: !!this.callingTables[msg.table], ...this.computeTableBill(msg.table) });
        }

        if (msg.type === "call_waiter" && msg.table) {
          this.callingTables[msg.table] = true;
          this.recognizeReturningGuest(msg.table, msg.deviceId);
          this.notify("waiter", { kind: "call_waiter", table: msg.table });
          this.log(conn, "call_waiter", { table: msg.table });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "guest_quick_request" && msg.table && msg.what) {
          const validWhats = ["cutlery", "napkins", "toothpicks", "salt_pepper", "order_problem"];
          if (!validWhats.includes(msg.what)) return;
          this.recognizeReturningGuest(msg.table, msg.deviceId);
          const openedBy = this.openTables[msg.table] ? this.openTables[msg.table].openedBy : null;
          const payload = { kind: "quick_request", what: msg.what, table: msg.table };
          if (openedBy) this.notifyStaff(openedBy, payload);
          else this.notify("waiter", payload);
          this.log(conn, "guest_quick_request", { table: msg.table, what: msg.what });
          await this.persist();
        }

        if (msg.type === "guest_repeat_request" && msg.table && Array.isArray(msg.items) && msg.items.length) {
          const items = msg.items
            .filter(i => i && typeof i.name === "string" && (i.dest === "kitchen" || i.dest === "bar") && Number(i.qty) > 0)
            .slice(0, 20)
            .map(i => ({ name: String(i.name).slice(0, 60), dest: i.dest, qty: Math.min(Math.floor(Number(i.qty)), 20) }));
          if (items.length === 0) return;
          this.recognizeReturningGuest(msg.table, msg.deviceId);
          const req = { id: crypto.randomUUID(), table: msg.table, items, requestedAt: Date.now() };
          this.repeatRequests.push(req);
          const openedBy = this.openTables[msg.table] ? this.openTables[msg.table].openedBy : null;
          const payload = { kind: "repeat_request", table: msg.table, requestId: req.id };
          if (openedBy) this.notifyStaff(openedBy, payload);
          else this.notify("waiter", payload);
          this.log(conn, "guest_repeat_request", { table: msg.table });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "acknowledge_table" && msg.table && conn.role === "waiter") {
          let changed = false;
          if (this.callingTables[msg.table]) { delete this.callingTables[msg.table]; changed = true; }
          if (this.billRequestedTables[msg.table]) { delete this.billRequestedTables[msg.table]; changed = true; }
          if (this.returningGuestTables[msg.table]) { delete this.returningGuestTables[msg.table]; changed = true; }
          if (this.tableGuestDevice[msg.table]) { delete this.tableGuestDevice[msg.table]; changed = true; } // the "usual order" note is a one-time heads-up, not a standing fact — once a waiter has picked up this table, it's served its purpose
          const before = this.pendingBillEscalations.length;
          this.pendingBillEscalations = this.pendingBillEscalations.filter(e => e.table !== msg.table);
          if (this.pendingBillEscalations.length !== before) changed = true;
          if (changed) { await this.persist(); this.broadcast(); }
        }

        if (msg.type === "request_bill" && msg.table && this.openTables[msg.table]) {
          this.billRequestedTables[msg.table] = true;
          const openedBy = this.openTables[msg.table] ? this.openTables[msg.table].openedBy : null;
          if (openedBy) this.notifyStaff(openedBy, { kind: "request_bill", table: msg.table });
          else this.notify("waiter", { kind: "request_bill", table: msg.table });
          this.log(conn, "request_bill", { table: msg.table });
          await this.scheduleBillEscalation(msg.table);
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "save_guest_order" && msg.deviceId && msg.table) {
          const session = this.openTables[msg.table];
          if (session) {
            const rounds = this.history.filter(h => h.table === msg.table && h.servedAt >= session.openedAt);
            const activeOrders = this.orders.filter(o => o.table === msg.table);
            const merged = {};
            const addItems = (items, dest) => {
              (items || []).forEach(i => {
                const key = dest + "|" + i.name;
                if (!merged[key]) merged[key] = { name: i.name, dest, qty: 0 };
                merged[key].qty += i.qty;
              });
            };
            rounds.forEach(r => { addItems(billable(r.kitchenItems), "kitchen"); addItems(billable(r.barItems), "bar"); });
            activeOrders.forEach(o => { addItems(billable(o.kitchenItems), "kitchen"); addItems(billable(o.barItems), "bar"); });
            const items = Object.values(merged);
            if (items.length > 0) {
              const entry = { items, ts: Date.now() };
              const list = this.guestHistory[msg.deviceId] || [];
              list.push(entry);
              this.guestHistory[msg.deviceId] = list.slice(-5); // keep the last 5 visits only
              this.tableGuestDevice[msg.table] = msg.deviceId;
              await this.persist();
            }
          }
        }

        if (msg.type === "forget_guest" && msg.deviceId) {
          delete this.guestHistory[msg.deviceId];
          for (const t of Object.keys(this.tableGuestDevice)) {
            if (this.tableGuestDevice[t] === msg.deviceId) delete this.tableGuestDevice[t];
          }
          await this.persist();
        }

        if (msg.type === "get_guest_history_for_table" && msg.table && (conn.role === "waiter" || conn.role === "manager")) {
          const deviceId = this.tableGuestDevice[msg.table];
          const history = deviceId ? (this.guestHistory[deviceId] || []) : [];
          this.sendTo(conn, { type: "guest_history_result", table: msg.table, history });
          if (deviceId) {
            // One-time heads-up: once a waiter has been shown this, don't
            // keep showing it — clearing here (not just on acknowledge)
            // means it can't be orphaned by callingTables/returningGuestTables
            // having already been cleared through some other path.
            delete this.tableGuestDevice[msg.table];
            await this.persist();
          }
        }

        if (msg.type === "register_push" && msg.pushId && msg.subscription) {
          const endpoint = msg.subscription.endpoint;
          this.pushSubs = this.pushSubs.filter(p =>
            p.id !== msg.pushId && (!endpoint || !p.subscription || p.subscription.endpoint !== endpoint)
          ); // a browser has exactly one real push subscription — stale role-bindings from testing other pages on this device get dropped here
          this.pushSubs.push({ id: msg.pushId, role: conn.role, staffId: conn.staffId || null, subscription: msg.subscription });
          console.log(`[push] register_push: role=${conn.role} staffId=${conn.staffId || null} pushId=${msg.pushId} total=${this.pushSubs.length}`);
          this.sendTo(conn, { type: "push_registered", total: this.pushSubs.length });
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

        if (msg.type === "set_staff_pin") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff && conn.staffId === msg.staffId) {
            const newPin = String(msg.newPin || "").trim();
            if (/^\d{4,6}$/.test(newPin)) {
              staff.pin = newPin;
              this.log(conn, "set_staff_pin", {});
              await this.persist();
              this.sendTo(conn, { type: "staff_pin_changed" });
            } else {
              this.sendTo(conn, { type: "staff_pin_change_error" });
            }
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

        if (msg.type === "set_wifi" && conn.role === "manager") {
          this.wifi = {
            enabled: !!msg.enabled,
            ssid: String(msg.ssid || "").slice(0, 60),
            password: String(msg.password || "").slice(0, 80),
          };
          this.log(conn, "set_wifi", { enabled: this.wifi.enabled });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "create_staff" && conn.role === "manager") {
          if (msg.role === "manager" && !conn.isMaster) return; // only the master manager can appoint other managers
          if (msg.role === "monitor") return; // the monitor role is never created through the manager UI — it's a hidden, bootstrap-only account
          const staff = { id: crypto.randomUUID(), role: msg.role, name: "", pin: "1111" };
          if (msg.role === "manager") staff.isMaster = false; // newly appointed managers start as regular managers
          this.staff.push(staff);
          this.log(conn, "create_staff", { role: msg.role });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "set_notify_prefs" && conn.role === "monitor" && msg.prefs) {
          const staff = this.staff.find(s => s.id === conn.staffId);
          if (staff) {
            staff.notifyPrefs = {};
            NOTIFY_CATEGORIES.forEach(c => { staff.notifyPrefs[c] = !!msg.prefs[c]; });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "delete_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff && staff.role === "manager" && !conn.isMaster) return; // only the master manager can remove another manager
          if (staff && staff.role === "monitor") return; // never deletable through the manager UI/API
          this.staff = this.staff.filter(s => s.id !== msg.staffId);
          this.pushSubs = this.pushSubs.filter(p => p.staffId !== msg.staffId); // stop notifying a device once its staff record is gone
          this.log(conn, "delete_staff", { role: staff ? staff.role : null, name: staff ? staff.name : null });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "reassign_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff && staff.role === "manager" && !conn.isMaster) return; // only the master manager can move another manager's device link
          if (staff && staff.role === "monitor") return; // never reassignable through the manager UI/API — would also rotate the creator's own fixed access link
          if (staff) {
            this.pushSubs = this.pushSubs.filter(p => p.staffId !== staff.id); // old device's push binding no longer applies once the id rotates
            staff.id = crypto.randomUUID(); // old device's cached id stops matching anyone
            this.log(conn, "reassign_staff", { role: staff.role, name: staff.name });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "promote_master" && conn.role === "manager" && conn.isMaster && msg.staffId) {
          // Hands mastership to another manager — exactly one master at a
          // time, so the outgoing master demotes automatically.
          const target = this.staff.find(s => s.id === msg.staffId && s.role === "manager");
          if (target) {
            this.staff.forEach(s => { if (s.role === "manager") s.isMaster = false; });
            target.isMaster = true;
            this.log(conn, "promote_master", { name: target.name });
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

        if (msg.type === "assign_qr" && conn.role === "manager" && msg.qrId && msg.table) {
          const valid = await verifyQrId(msg.qrId, this.env.QR_SIGNING_SECRET);
          if (!valid) {
            this.sendTo(conn, { type: "qr_assign_error", reason: "not_ours" });
            return;
          }
          this.qrAssignments[msg.qrId] = parseInt(msg.table, 10);
          this.log(conn, "assign_qr", { qrId: msg.qrId, table: this.qrAssignments[msg.qrId] });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "generate_qr" && conn.role === "manager") {
          // Mints a brand-new table QR id, signed with our secret, so the
          // manager can produce additional table QR codes from the app
          // itself without needing a fresh batch generated externally.
          const qrId = await signQrId(this.env.QR_SIGNING_SECRET);
          this.sendTo(conn, { type: "qr_generated", qrId });
        }

        if (msg.type === "unassign_qr" && conn.role === "manager" && msg.qrId) {
          delete this.qrAssignments[msg.qrId];
          this.log(conn, "unassign_qr", { qrId: msg.qrId });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "add_menu_category" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = { id: crypto.randomUUID(), title: { ru: String(msg.title || "Раздел").slice(0, 40) }, items: [] };
          setLang(cat.title, "kk", msg.title_kk, 40);
          this.menu[dest].push(cat);
          this.log(conn, "add_menu_category", { dest, title: cat.title.ru });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "rename_menu_category" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          if (cat) {
            const newTitle = String(msg.title || cat.title.ru).slice(0, 40);
            if (newTitle !== cat.title.ru && msg.title_kk === undefined) { delete cat.title.kk; delete cat.title.en; } // old translation no longer matches
            cat.title.ru = newTitle;
            setLang(cat.title, "kk", msg.title_kk, 40);
            this.log(conn, "rename_menu_category", { dest, title: cat.title.ru });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "delete_menu_category" && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          for (const item of (cat && cat.items) || []) await this.deleteItemImages(item);
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
            if (msg.price2) item.price2 = String(msg.price2).slice(0, 20);
            if (msg.price3) item.price3 = String(msg.price3).slice(0, 20);
            if (msg.desc) item.desc = { ru: String(msg.desc).slice(0, 200) };
            setLang(item.name, "kk", msg.name_kk, 80);
            if (item.desc) setLang(item.desc, "kk", msg.desc_kk, 400);
            const parts = sanitizeParts(msg.parts, dest);
            if (parts.length) item.parts = parts;
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
            if (msg.name !== undefined) {
              const newName = String(msg.name).slice(0, 60) || item.name.ru;
              if (newName !== item.name.ru && msg.name_kk === undefined) { delete item.name.kk; delete item.name.en; }
              item.name.ru = newName;
            }
            setLang(item.name, "kk", msg.name_kk, 80);
            if (msg.price !== undefined) item.price = String(msg.price).slice(0, 20) || item.price;
            if (msg.price2 !== undefined) {
              if (msg.price2) item.price2 = String(msg.price2).slice(0, 20);
              else delete item.price2; // empty means "back to a single price"
            }
            if (msg.price3 !== undefined) {
              if (msg.price3) item.price3 = String(msg.price3).slice(0, 20);
              else delete item.price3;
            }
            if (msg.desc !== undefined) {
              const newDesc = String(msg.desc || "").slice(0, 200);
              if (newDesc) {
                if (!item.desc || item.desc.ru !== newDesc) {
                  if (msg.desc_kk === undefined) item.desc = { ru: newDesc }; // drop translations of the old text
                  else { item.desc = item.desc || {}; item.desc.ru = newDesc; }
                }
              }
              else delete item.desc;
            }
            if (item.desc) setLang(item.desc, "kk", msg.desc_kk, 400);
            if (msg.parts !== undefined) {
              const parts = sanitizeParts(msg.parts, dest);
              if (parts.length) item.parts = parts;
              else delete item.parts; // empty means "not a combo"
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
            if (item) await this.deleteItemImages(item);
            cat.items = cat.items.filter(i => i.id !== msg.itemId);
            this.log(conn, "delete_menu_item", { dest, name: item ? item.name.ru : null });
            await this.persist();
            this.broadcast();
          }
        }

        if ((msg.type === "upload_menu_image" || msg.type === "remove_menu_image") && conn.role === "manager") {
          const dest = msg.dest === "bar" ? "bar" : "kitchen";
          const cat = this.menu[dest].find(c => c.id === msg.categoryId);
          const item = cat && cat.items.find(i => i.id === msg.itemId);
          if (!item) return;
          if (msg.type === "remove_menu_image") {
            await this.deleteItemImages(item);
            this.log(conn, "remove_menu_image", { dest, name: item.name.ru });
          } else {
            const full = dataUrlToBytes(msg.full);
            const thumb = dataUrlToBytes(msg.thumb) || full;
            if (!full || !thumb) {
              this.sendTo(conn, { type: "image_error", itemId: item.id, reason: "bad_image" });
              return;
            }
            await this.deleteItemImages(item);
            item.imgFull = await this.putImage(full.bytes, full.type, full.ext);
            item.img = await this.putImage(thumb.bytes, thumb.type, thumb.ext);
            this.log(conn, "upload_menu_image", { dest, name: item.name.ru });
            this.sendTo(conn, { type: "image_saved", itemId: item.id });
          }
          await this.persist();
          this.broadcast();
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

        if (msg.type === "confirm_repeat_request" && (conn.role === "waiter" || conn.role === "manager") && msg.requestId) {
          const req = this.repeatRequests.find(r => r.id === msg.requestId);
          if (req) {
            this.repeatRequests = this.repeatRequests.filter(r => r.id !== msg.requestId);
            const isAvailable = (dest, name) => this.isOrderable(dest, name);
            const priceFor = (dest, name) => {
              const cat = (this.menu[dest] || []).find(c => c.items.some(i => i.name.ru === name));
              const item = cat && cat.items.find(i => i.name.ru === name);
              return item ? item.price : "0";
            };
            const { kitchenItems, barItems } = this.expandCombos(
              req.items.filter(i => i.dest === "kitchen" && isAvailable("kitchen", i.name))
                .map(i => ({ name: i.name, qty: i.qty, price: priceFor("kitchen", i.name), note: "" })),
              req.items.filter(i => i.dest === "bar" && isAvailable("bar", i.name))
                .map(i => ({ name: i.name, qty: i.qty, price: priceFor("bar", i.name), note: "" })));
            if (!this.openTables[req.table]) {
              this.openTables[req.table] = { openedAt: Date.now(), openedBy: conn.staffId || null };
            }
            const order = {
              id: crypto.randomUUID(),
              table: req.table,
              createdAt: Date.now(),
              waiterName: conn.staffName || "",
              waiterId: conn.staffId || null,
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
            this.log(conn, "confirm_repeat_request", { table: req.table });
            await this.persist();
            this.broadcast();
            if (order.kitchenStatus === "pending") this.notify("cook", { table: order.table, orderId: order.id });
            if (order.barStatus === "pending") this.notify("bartender", { table: order.table, orderId: order.id });
          }
        }

        if (msg.type === "reject_repeat_request" && (conn.role === "waiter" || conn.role === "manager") && msg.requestId) {
          this.repeatRequests = this.repeatRequests.filter(r => r.id !== msg.requestId);
          this.log(conn, "reject_repeat_request", {});
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "new_order") {
          const table = msg.table;
          if (!this.openTables[table]) {
            this.openTables[table] = { openedAt: Date.now(), openedBy: conn.staffId || null };
          }

          const isAvailable = (dest, name) => this.isOrderable(dest, name);
          const rawKitchen = (msg.kitchenItems || []).filter(i => !i.component);
          const rawBar = (msg.barItems || []).filter(i => !i.component);
          const { kitchenItems, barItems } = this.expandCombos(
            rawKitchen.filter(i => isAvailable("kitchen", i.name)),
            rawBar.filter(i => isAvailable("bar", i.name)));
          const removed = [
            ...rawKitchen.filter(i => !isAvailable("kitchen", i.name)).map(i => i.name),
            ...rawBar.filter(i => !isAvailable("bar", i.name)).map(i => i.name),
          ];

          const order = {
            id: crypto.randomUUID(),
            table,
            createdAt: Date.now(),
            waiterName: conn.staffName || "",
            waiterId: conn.staffId || null,
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
            const wasFullyReady = this.isOrderFullyReady(order);
            order.kitchenStatus = "ready";
            order.kitchenReadyAt = Date.now();
            this.consumeStock("kitchen", order.kitchenItems, "cook");
            this.log(conn, "kitchen_ready", { table: order.table });
            const nowFullyReady = this.isOrderFullyReady(order);
            if (nowFullyReady && !wasFullyReady) {
              order.fullyReadyAt = Date.now();
              if (order.waiterId) this.notifyStaff(order.waiterId, { table: order.table, orderId: order.id, part: "kitchen" });
              else this.notify("waiter", { table: order.table, orderId: order.id, part: "kitchen" });
              await this.scheduleEscalation(order.id, order.table);
            } else {
              this.notify("waiter", { table: order.table, orderId: order.id, part: "kitchen" });
            }
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "bar_ready") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            const wasFullyReady = this.isOrderFullyReady(order);
            order.barStatus = "ready";
            order.barReadyAt = Date.now();
            this.consumeStock("bar", order.barItems, "bartender");
            this.log(conn, "bar_ready", { table: order.table });
            const nowFullyReady = this.isOrderFullyReady(order);
            if (nowFullyReady && !wasFullyReady) {
              order.fullyReadyAt = Date.now();
              if (order.waiterId) this.notifyStaff(order.waiterId, { table: order.table, orderId: order.id, part: "bar" });
              else this.notify("waiter", { table: order.table, orderId: order.id, part: "bar" });
              await this.scheduleEscalation(order.id, order.table);
            } else {
              this.notify("waiter", { table: order.table, orderId: order.id, part: "bar" });
            }
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "request_cancel_order") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            const reqId = crypto.randomUUID();
            this.cancelRequests.push({
              id: reqId,
              orderId: order.id,
              table: order.table,
              requestedBy: conn.staffName || "",
              requestedAt: Date.now(),
            });
            this.log(conn, "request_cancel_order", { table: order.table });
            await this.persist();
            this.broadcast();
            this.notify("manager", { kind: "cancel_request", table: order.table, requestId: reqId });
          }
        }

        if (msg.type === "approve_cancel" && conn.role === "manager") {
          const idx = this.cancelRequests.findIndex(r => r.id === msg.requestId);
          if (idx !== -1) {
            const req = this.cancelRequests[idx];
            this.cancelRequests.splice(idx, 1);
            const order = this.orders.find(o => o.id === req.orderId);
            this.orders = this.orders.filter(o => o.id !== req.orderId);
            this.log(conn, "approve_cancel", { table: req.table });
            await this.persist();
            this.broadcast();
            if (order) {
              if (order.kitchenItems.length) this.notify("cook", { table: order.table, orderId: order.id, cancelled: true });
              if (order.barItems.length) this.notify("bartender", { table: order.table, orderId: order.id, cancelled: true });
              this.notify("waiter", { kind: "cancel_approved", table: req.table });
            } else {
              this.notify("waiter", { kind: "cancel_already_resolved", table: req.table });
            }
          }
        }

        if (msg.type === "reject_cancel" && conn.role === "manager") {
          const idx = this.cancelRequests.findIndex(r => r.id === msg.requestId);
          if (idx !== -1) {
            const req = this.cancelRequests[idx];
            this.cancelRequests.splice(idx, 1);
            this.log(conn, "reject_cancel", { table: req.table });
            await this.persist();
            this.broadcast();
            this.notify("waiter", { kind: "cancel_rejected", table: req.table });
          }
        }

        if (msg.type === "served") {
          const order = this.orders.find(o => o.id === msg.orderId);
          this.orders = this.orders.filter(o => o.id !== msg.orderId);
          this.cancelRequests = this.cancelRequests.filter(r => r.orderId !== msg.orderId); // no longer relevant — order is done
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
            billable(r.kitchenItems).forEach(i => {
              const key = "kitchen|" + i.name;
              if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price };
              merged[key].qty += i.qty;
            });
            billable(r.barItems).forEach(i => {
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
          delete this.billRequestedTables[table];
          delete this.returningGuestTables[table];
          delete this.tableGuestDevice[table]; // closing the table ends this dining session — the next party at this table shouldn't inherit the last guest's identity
          this.pendingBillEscalations = this.pendingBillEscalations.filter(e => e.table !== table);
          this.closedTables.push(receipt);
          if (this.closedTables.length > CLOSED_TABLES_LIMIT) this.closedTables = this.closedTables.slice(-CLOSED_TABLES_LIMIT);
          this.log(conn, "close_table", { table, total });

          await this.persist();
          this.broadcast();
          this.sendTo(conn, { type: "table_closed", receipt });
          this.notifyGuestAtTable(table, { type: "table_closed" });
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

    // Manager-uploaded menu photos: /img/<venue>/<uuid>.<ext>, stored in that
    // venue's Durable Object. Keys are random and never reused -> immutable.
    if (url.pathname.startsWith("/img/") && request.method === "GET") {
      const key = decodeURIComponent(url.pathname.slice(5));
      if (!IMG_KEY_RE.test(key)) return new Response("Not found", { status: 404, headers: cors });
      const venue = key.split("/")[0];
      const cache = caches.default;
      const hit = await cache.match(request);
      if (hit) return hit;
      const stub = env.ORDER_BOARD.get(env.ORDER_BOARD.idFromName(venue));
      const fwd = new Request(request);
      fwd.headers.set("X-Venue", venue);
      const resp = await stub.fetch(fwd);
      if (resp.ok) await cache.put(request, resp.clone());
      return resp;
    }

    if (url.pathname === "/ws") {
      // Multi-venue support: a venue's own frontend appends ?venue=<slug> to
      // its WS URL, giving it a completely separate Durable Object (its own
      // staff/menu/tables/orders) under the exact same code. No venue param
      // — including every GO pub file today — resolves to "main", so GO
      // pub's existing address and data are entirely untouched by this.
      let venue = (url.searchParams.get("venue") || "main").slice(0, 60);
      if (!/^[a-z0-9-]+$/.test(venue)) venue = "main"; // reject anything odd rather than let it become a stray DO
      const id = env.ORDER_BOARD.idFromName(venue);
      const stub = env.ORDER_BOARD.get(id);
      const fwd = new Request(request);
      fwd.headers.set("X-Venue", venue);
      return stub.fetch(fwd);
    }

    return new Response("GO pub order board is running.", { headers: cors });
  },
};
