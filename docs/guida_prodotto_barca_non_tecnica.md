# Guida Non Tecnica al Prodotto BARCA

Versione documento: 2026-03-25

## 1. A cosa serve questo prodotto

BARCA e' un sistema unico che aiuta a:

- leggere i file esportati dal gestionale;
- pulire e normalizzare i dati;
- suggerire trasferimenti di merce tra negozi;
- stimare i riordini;
- salvare ogni aggiornamento in storico;
- mostrare tutto in una dashboard web;
- creare cataloghi consultabili ed esportabili.

In pratica, il prodotto risponde a tre domande:

1. Dove abbiamo troppa o poca merce?
2. Cosa conviene spostare tra i negozi?
3. Cosa conviene riordinare per la stagione successiva o continuativa?


## 2. Flusso semplice del sistema

Il flusso logico e' questo:

1. Entrano i file raw dal gestionale.
2. Il sistema li riconosce e li mette al posto giusto.
3. Vengono estratti venduto, consegnato, stock, sellout e altri dati.
4. Viene calcolata una domanda stimata per ogni articolo-negozio.
5. Il sistema propone trasferimenti tra negozi.
6. Se abilitato, produce anche forecast ordini.
7. Tutto viene salvato nel database con uno storico per run.
8. La dashboard mostra il risultato della run selezionata.


## 3. I moduli del prodotto

### 3.1 Ingest e pulizia dati

Questo modulo:

- riconosce i file corretti;
- converte eventuali Excel;
- uniforma formati numerici e codici negozio;
- produce file puliti usati dagli altri moduli.

Valore pratico:

- riduce errori manuali;
- rende confrontabili i dati di negozi e stagioni diverse;
- evita che i modelli lavorino su file sporchi o incoerenti.


### 3.2 Distribuzione giacenze

Questo e' il cuore operativo.

Il sistema prova a capire:

- dove un articolo rischia di mancare;
- dove un altro negozio ne ha in eccesso;
- se il trasferimento e' sostenibile dal punto di vista logistico.

Il risultato finale non e' solo "chi ha di piu' e chi ha di meno", ma una proposta filtrata da regole reali:

- priorita' ai negozi piu' importanti;
- protezione dei negozi donatori;
- rispetto della capacita' dei negozi;
- limiti realistici di movimentazione;
- gestione coerente della logistica.


### 3.3 Forecast ordini

Questo modulo stima quanta merce comprare.

Lavora in 3 modi:

- metodo matematico;
- metodo Random Forest, se c'e' abbastanza storico;
- metodo ibrido, che media metodo matematico e Random Forest.


### 3.4 Dashboard

La dashboard non e' un semplice cruscotto grafico: e' la sintesi della run selezionata.

Importante:

- ogni dashboard si riferisce a una run storica precisa;
- non e' "live" sul magazzino del momento;
- i numeri dipendono dalla qualita' dei file importati in quella run.


### 3.5 Catalogo

Il modulo catalogo:

- importa articoli, stagioni, prezzi e disponibilita';
- permette ricerca e consultazione;
- genera cataloghi HTML/ZIP/JPG da condividere.


## 4. Come ragionano i numeri principali della dashboard

Di seguito spiego i KPI piu' importanti in linguaggio business.

### 4.1 Sellout medio

Significato business:

- dice quanto velocemente la merce ruota rispetto al consegnato.

Idea semplice:

- sellout % = venduto / consegnato * 100

Come lavora questo sistema:

- il sellout viene letto dal report del gestionale;
- il valore viene limitato tra 0% e 100% per evitare anomalie estreme;
- in dashboard il KPI "Sellout medio" e' la media semplice dei sellout delle righe importate.

Formula effettiva dashboard:

- Sellout medio = media di Sellout_Clamped

Attenzione:

- non e' una media ponderata sul consegnato;
- quindi puo' essere utile per capire il tono generale, ma non sempre rappresenta perfettamente il peso economico o il peso in paia;
- se vuoi un KPI ancora piu' affidabile, in futuro conviene aggiungere anche un sellout ponderato.

Quando fidarsi:

- quando i file di partenza hanno consegnato e venduto coerenti;
- quando si usa il KPI come indicatore sintetico, non come unica verita'.


### 4.2 Paia da trasferire

Significato business:

- e' il volume totale dei trasferimenti suggeriti dal motore.

Formula effettiva dashboard:

- Paia da trasferire = somma di tutte le qty in fact_transfer_suggestion

Attenzione:

- sono trasferimenti suggeriti, non necessariamente gia' eseguiti.


### 4.3 Valore ordini

Significato business:

- e' il budget stimato per i riordini suggeriti.

Formula di base:

- Budget acquisto articolo = Quantita' da acquistare * Prezzo acquisto

Formula effettiva dashboard:

- Valore ordini = somma dei Budget_Acquisto del forecast ordini

Attenzione:

- e' una stima economica;
- non considera automaticamente eventuali trattative prezzo, minimi d'ordine, arrotondamenti commerciali o vincoli fornitore extra-codice.


### 4.4 Mancanza stimata totale

Significato business:

- misura quanta domanda stimata resta scoperta dopo i movimenti suggeriti.

Formula concettuale:

- Mancanza = max(Domanda stimata - Stock dopo movimenti, 0)

Formula effettiva dashboard:

- Mancanza stimata totale = somma di tutti i deficit positivi

Interpretazione:

- se vale 0, il sistema ritiene di aver coperto i fabbisogni stimati;
- se e' alta, ci sono ancora negozi-articolo dove la domanda prevista supera la disponibilita'.


### 4.5 Media paia per trasferimento

Formula:

- Media paia per trasferimento = Paia da trasferire / Numero righe trasferimento

Utilita':

- aiuta a capire se il piano e' fatto di pochi movimenti grandi o tanti movimenti piccoli.


### 4.6 Trend ultima stagione

Formula:

- Trend % = (volume stagione piu' recente - volume stagione precedente) / volume stagione precedente * 100

Utilita':

- serve a capire se l'ultima stagione trattata dal forecast ordini e' sopra o sotto la precedente.

Attenzione:

- se le stagioni confrontate non sono omogenee, il numero va letto con prudenza.


### 4.7 Candidati prossima stagione

Questa sezione cerca articoli continuativi che potrebbero avere senso anche per la stagione successiva.

Le logiche principali sono:

- si osserva il venduto della continuativa;
- si applica un fattore preso dalla stagione corrente;
- si confronta il risultato con la giacenza attuale.

Formule principali:

- Fattore applicato = media del rapporto tra forecast corrente e venduto nella finestra stagionale corrente, per categoria/tipologia
- Quantita' prevista prossima corrente = venduto nella finestra stagionale della continuativa * fattore_applicato
- Extra stimato = quantita' prevista - giacenza
- Budget previsto = quantita' prevista * prezzo acquisto
- Transition score = venduto nella finestra stagionale / (giacenza + 1)

Definizione pratica di "finestra stagionale":

- stagioni `I` e `Y`: dal `01/08` al `31/03`
- stagioni `E` e `G`: dal `01/02` al `30/10`

Quindi il campo non indica il venduto degli ultimi giorni, ma il venduto dell'articolo dentro il periodo commerciale della stagione.

Interpretazione:

- piu' il transition score e' alto, piu' l'articolo sembra "girare" rispetto allo stock.

Attenzione:

- e' una logica di supporto decisionale, non un ordine automatico definitivo.


## 5. Come funziona il motore di distribuzione

Il sistema non usa solo un modello matematico puro. Usa un approccio ibrido:

- parte algoritmica/regole business;
- parte AI prudente;
- vincoli logistici e operativi.

### 5.1 Domanda stimata con formula business

Il sistema costruisce una domanda osservata combinando:

- 60% del venduto di periodo;
- 25% del venduto totale;
- 15% del sellout applicato al consegnato.

Formula semplificata:

- Domanda osservata = 0.60 * Periodo_Qty + 0.25 * Venduto_Qty + 0.15 * (Sellout / 100) * Consegnato_Qty

Se la riga e' povera di segnali, usa dei prior:

- media dell'articolo;
- media del negozio.

Formula prior:

- Prior = 0.55 * media_articolo + 0.45 * media_negozio

Se non c'e' osservato utile:

- Domanda base = 0.35 * Prior


### 5.2 Correzione per scarsita'

Se un articolo:

- ha sellout alto;
- ha copertura stock bassa;

allora il sistema alza la domanda stimata.

Idea business:

- se un articolo gira bene e c'e' poca copertura, il bisogno reale probabilmente e' piu' alto del solo storico.


### 5.3 Correzione per fascia negozio

Ogni fascia ha un moltiplicatore di servizio:

- fascia 1 = 1.20
- fascia 2 = 1.15
- fascia 3 = 1.10
- fascia 4 = 1.06
- fascia 5 = 1.03
- fascia 6 = 1.00
- fascia 7 = 0.98

Interpretazione:

- i negozi piu' importanti vengono protetti di piu';
- il sistema e' intenzionalmente orientato a favorire le fasce alte.


### 5.4 Parte AI del modello domanda

La parte AI usa una regressione ridge con validazione out-of-fold.

In pratica:

- il sistema guarda molte caratteristiche insieme;
- prova a stimare una domanda "intelligente";
- ma non le lascia mai il pieno controllo.

Segnali usati dal modello AI:

- sellout;
- stock;
- profondita' taglie;
- media articolo;
- media negozio;
- numero negozi coperti;
- osservazioni di vendita;
- velocita' del negozio;
- fascia;
- fattore servizio;
- boost di scarsita'.

Punto molto importante:

- il peso massimo della parte AI e' limitato al 45%;
- quindi il sistema resta deliberatamente prudente e non e' una black box totale.

Formula finale:

- Domanda ibrida = (1 - peso_AI) * Domanda_regole + peso_AI * Domanda_AI

Questo e' positivo per la fiducia:

- il risultato non dipende mai solo da un modello AI puro;
- la componente business resta dominante.


### 5.5 Come nascono i trasferimenti

Dopo aver stimato la domanda, il sistema propone trasferimenti tenendo conto di:

- chi ha bisogno;
- chi puo' donare;
- sicurezza minima del donatore;
- limiti inbound e outbound;
- capacita' del negozio ricevente;
- regole speciali logistiche;
- eccezioni come magazzino e canale web.

Quindi un articolo non viene spostato solo perche' "altrove ce n'e' di piu'".
Viene spostato se l'operazione resta sensata nel contesto operativo.


## 6. Come funziona il forecast ordini

### 6.1 Metodo matematico

E' il metodo piu' semplice e leggibile.

Logica:

- si parte dal venduto;
- si applica un fattore di copertura;
- si confronta il target con la giacenza;
- si ottiene il fabbisogno da acquistare.

Formule:

- Predizione vendite = venduto totale oppure venduto nella finestra stagionale, a seconda del modulo
- Stock target = Predizione vendite * fattore copertura
- Da acquistare = max(0, Stock target - Giacenza)

Nel progetto il fattore copertura di default e' 1.20.

Poi il totale viene distribuito sulle taglie in base al mix storico di vendita.

Pregio:

- molto leggibile e difendibile in riunione.

Limite:

- meno sensibile a pattern complessi.


### 6.2 Metodo Random Forest

Viene usato solo se c'e' abbastanza storico.

Usa:

- categoria;
- tipologia;
- marchio;
- colore;
- materiale;
- medie pesate delle stagioni precedenti;
- un flag di crescita esplosiva.

Pregio:

- puo' cogliere pattern piu' complessi rispetto al metodo matematico.

Limite:

- e' meno intuitivo da spiegare articolo per articolo;
- su articoli nuovi o dati deboli puo' essere meno robusto.


### 6.3 Metodo ibrido ordini

Il metodo ibrido ordini fa una media tra:

- forecast matematico;
- forecast Random Forest.

Formula semplificata:

- Forecast ibrido = media tra forecast math e forecast RF

Vale sia per il totale sia per la distribuzione taglie.

Pregio:

- evita di affidarsi troppo a un solo approccio.


## 7. Dove il sistema e' forte

Le aree in cui il prodotto e' gia' forte sono:

- integrazione end-to-end, dal file raw alla dashboard;
- storico run con database;
- logica trasferimenti molto aderente al business;
- modello domanda prudente e non completamente black box;
- forecast ordini con piu' metodi;
- catalogo integrato con export.


## 8. Dove conviene fidarsi di piu'

Ti puoi fidare di piu' quando:

- i file sorgente sono corretti e aggiornati;
- le stagioni confrontate sono coerenti;
- gli articoli hanno un minimo di storico;
- i negozi sono configurati bene per fascia e capacita';
- il risultato viene usato come supporto decisionale, non come automatismo cieco.

In queste situazioni il prodotto e' molto utile per:

- priorizzare problemi;
- vedere dove manca merce;
- capire dove ci sono eccessi;
- stimare ordini in modo ordinato e replicabile.


## 9. Dove conviene essere prudenti

Serve prudenza quando:

- arrivano file sporchi o con formato cambiato;
- il sellout nel report origine e' incoerente;
- un articolo e' nuovo e ha poco storico;
- c'e' una promo, una rottura di stock o un evento eccezionale non presente nei dati;
- i prezzi acquisto non sono aggiornati;
- si vuole leggere il sellout medio come verita' economica assoluta.

In particolare:

- il sellout medio dashboard e' una media semplice, non ponderata;
- i trasferimenti sono suggerimenti, non esecuzioni reali;
- il forecast ordini e' una stima, non un impegno commerciale finale.


## 10. Cose da migliorare per aumentare fiducia e qualita'

Di seguito le migliorie piu' utili in ottica manageriale.

### Priorita' alta

- aggiungere un sellout medio ponderato sul consegnato;
- mostrare per ogni KPI la formula direttamente in dashboard;
- introdurre un indice di affidabilita' per articolo o per run;
- fare backtest automatici tra forecast e venduto reale;
- segnalare in modo esplicito quando un dato nasce da storico debole.

### Priorita' media

- distinguere meglio effetto promo da domanda strutturale;
- gestire eventi eccezionali o correzioni manuali guidate;
- dare spiegazioni piu' dettagliate sui motivi di ogni trasferimento;
- evidenziare meglio quali numeri sono "suggeriti", "stimati" o "consuntivi".

### Priorita' organizzativa

- definire una routine di controllo qualita' dei file prima della run;
- decidere una policy chiara per approvare o correggere trasferimenti e ordini;
- condividere con il team una legenda unica dei KPI.


## 11. Come leggere il prodotto in modo corretto

Il modo giusto di usare BARCA non e':

- "dice la verita' assoluta, quindi eseguiamo tutto".

Il modo giusto e':

- "ci da una base numerica coerente, storicizzata e veloce per prendere decisioni migliori".

Quindi il prodotto va visto come:

- molto buono per priorizzare;
- buono per stimare;
- ottimo per standardizzare il processo;
- da affiancare comunque a verifica commerciale e operativa.


## 12. Sintesi finale molto breve

Se devo riassumerlo in poche righe:

- il prodotto legge dati di vendita e stock;
- costruisce una domanda stimata prudente;
- propone trasferimenti realistici;
- stima riordini;
- salva lo storico;
- mostra tutto in una dashboard;
- aiuta a decidere meglio, ma non sostituisce il giudizio business.

La parte positiva principale e' questa:

- non e' un sistema "magico";
- e' un sistema ibrido, con regole business forti e AI controllata.

La principale area da migliorare per aumentare ancora la fiducia e' questa:

- spiegare sempre meglio in dashboard da dove nasce ogni numero e con quale affidabilita'.
