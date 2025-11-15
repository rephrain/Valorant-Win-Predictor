from bs4 import BeautifulSoup

def parse_k_to_int(s):
    """Convert '0.2k' -> 200, '15.8k' -> 15800."""
    s = s.lower().replace("k", "")
    return int(float(s) * 1000)

def classify_econ(symbol):
    """Map $, $$, $$$, blank to econ."""
    if symbol == "":
        return "eco"
    if symbol == "$":
        return "semi_eco"
    if symbol == "$$":
        return "semi_buy"
    if symbol == "$$$":
        return "full_buy"
    return None

def scrape_econ_table(html, team1_id, team2_id):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="wf-table-inset")

    all_rounds = []

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # --- Determine winner team ---
        # If inside <td> there is "mod-win mod-t" => team1 wins
        # If inside <td> there is "mod-win mod-ct" => team2 wins
        def detect_winner(td):
            sq = td.find("div", class_="mod-win")
            if not sq:
                return None
            classes = sq.get("class", [])
            if "mod-t" in classes:
                return team1_id
            if "mod-ct" in classes:
                return team2_id
            return None

        # Identify which row belongs to team1 or team2
        # Row 1 (first <tr>) = team1
        # Row 2 (second <tr>) = team2
        row_is_team1 = tr == table.find_all("tr")[0]

        for td in tds[1:]:
            round_num = td.find("div", class_="round-num")
            if not round_num:
                continue

            round_number = int(round_num.text.strip())

            # banks (always 2 bank entries inside each td)
            bank_divs = td.find_all("div", class_="bank")
            bank_start = parse_k_to_int(bank_divs[0].text.strip())
            bank_end = parse_k_to_int(bank_divs[1].text.strip())

            # loadouts = the <div class="rnd-sq">
            sqs = td.find_all("div", class_="rnd-sq")
            loadouts = [sq.get("title", "") for sq in sqs]

            # econ symbol from inside rnd-sq content
            econ_symbol = ""
            for sq in sqs:
                if sq.text.strip() in ["$", "$$", "$$$"]:
                    econ_symbol = sq.text.strip()
                    break
            econ_type = classify_econ(econ_symbol)

            # winner detection
            winner = detect_winner(td)

            # Build record
            data = {
                "round": round_number,
                "team": team1_id if row_is_team1 else team2_id,
                "bank_start": bank_start,
                "bank_end": bank_end,
                "loadouts": loadouts,
                "econ": econ_type,
                "winner_team": winner
            }

            all_rounds.append(data)

    return all_rounds

html_string = '''
<div style="padding: 20px 0; overflow-x: auto;">
				<table class="wf-table-inset mod-econ">
																	<tbody><tr>
							
							<td>
								
								<div class="ge-text-light label" style="padding-bottom: 10px;">	
									(BANK)
								</div>

								<div class="team" style="height: 28px;">
									
																					<img src="//owcdn.net/img/62bbebb185a7e.png">
										
																		PRX								</div>
								<div class="team" style="height: 28px; margin-top: 3px;">
																			
																					<img src="//owcdn.net/img/633822848a741.png">
										
																		G2								</div>

								<div class="ge-text-light label" style="padding-top: 10px;">	
									(BANK)
								</div>

							</td>
							
																								<td>

										<div class="ge-text-light round-num">
											1										</div>
										<div class="bank">0.2k</div>

										
											<div class="rnd-sq " title="4000">
																							</div>
										
											<div class="rnd-sq mod-win mod-t" title="4150">
																							</div>
										
										
										
									<div class="bank">
											0.3k										</div></td>
																																<td>

										<div class="ge-text-light round-num">
											2										</div>
										<div class="bank">9.3k</div>

										
											<div class="rnd-sq mod-win mod-ct" title="1100">
																							</div>
										
											<div class="rnd-sq " title="18300">
												$$											</div>
										
										<div class="bank">
											2.7k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											3										</div>
										<div class="bank">8.2k</div>

										
											<div class="rnd-sq mod-win mod-ct" title="22050">
												$$$											</div>
										
											<div class="rnd-sq " title="5800">
												$											</div>
										
										<div class="bank">
											8.1k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											4										</div>
										<div class="bank">4.6k</div>

										
											<div class="rnd-sq " title="24300">
												$$$											</div>
										
											<div class="rnd-sq mod-win mod-t" title="22650">
												$$$											</div>
										
										<div class="bank">
											1.0k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											5										</div>
										<div class="bank">8.3k</div>

										
											<div class="rnd-sq " title="11950">
												$$											</div>
										
											<div class="rnd-sq mod-win mod-t" title="21400">
												$$$											</div>
										
										<div class="bank">
											7.9k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											6										</div>
										<div class="bank">1.4k</div>

										
											<div class="rnd-sq mod-win mod-ct" title="20650">
												$$$											</div>
										
											<div class="rnd-sq " title="24100">
												$$$											</div>
										
										<div class="bank">
											5.5k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											7										</div>
										<div class="bank">7.6k</div>

										
											<div class="rnd-sq " title="21000">
												$$$											</div>
										
											<div class="rnd-sq mod-win mod-t" title="7900">
												$											</div>
										
										<div class="bank">
											8.4k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											8										</div>
										<div class="bank">6.6k</div>

										
											<div class="rnd-sq mod-win mod-ct" title="11950">
												$$											</div>
										
											<div class="rnd-sq " title="22800">
												$$$											</div>
										
										<div class="bank">
											6.1k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											9										</div>
										<div class="bank">15.8k</div>

										
											<div class="rnd-sq mod-win mod-ct" title="24300">
												$$$											</div>
										
											<div class="rnd-sq " title="8050">
												$											</div>
										
										<div class="bank">
											10.1k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											10										</div>
										<div class="bank">25.0k</div>

										
											<div class="rnd-sq " title="25800">
												$$$											</div>
										
											<div class="rnd-sq mod-win mod-t" title="21400">
												$$$											</div>
										
										<div class="bank">
											2.9k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											11										</div>
										<div class="bank">10.7k</div>

										
											<div class="rnd-sq " title="25000">
												$$$											</div>
										
											<div class="rnd-sq mod-win mod-t" title="23000">
												$$$											</div>
										
										<div class="bank">
											5.5k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											12										</div>
										<div class="bank">3.1k</div>

										
											<div class="rnd-sq mod-win mod-ct" title="24650">
												$$$											</div>
										
											<div class="rnd-sq " title="26500">
												$$$											</div>
										
										<div class="bank">
											6.1k										</div>
										
									</td>
																					</tr>
																	<tr>
							
							<td>
								
								<div class="ge-text-light label" style="padding-bottom: 10px;">	
									(BANK)
								</div>

								<div class="team" style="height: 28px;">
									
																					<img src="//owcdn.net/img/62bbebb185a7e.png">
										
																		PRX								</div>
								<div class="team" style="height: 28px; margin-top: 3px;">
																			
																					<img src="//owcdn.net/img/633822848a741.png">
										
																		G2								</div>

								<div class="ge-text-light label" style="padding-top: 10px;">	
									(BANK)
								</div>

							</td>
							
																								<td>

										<div class="ge-text-light round-num">
											13										</div>
										<div class="bank">0.2k</div>

										
											<div class="rnd-sq " title="4150">
																							</div>
										
											<div class="rnd-sq mod-win mod-ct" title="3700">
																							</div>
										
										<div class="bank">
											0.4k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											14										</div>
										<div class="bank">9.0k</div>

										
											<div class="rnd-sq " title="2450">
																							</div>
										
											<div class="rnd-sq mod-win mod-ct" title="15350">
												$$											</div>
										
										<div class="bank">
											4.0k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											15										</div>
										<div class="bank">0.7k</div>

										
											<div class="rnd-sq " title="21450">
												$$$											</div>
										
											<div class="rnd-sq mod-win mod-ct" title="16150">
												$$											</div>
										
										<div class="bank">
											18.3k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											16										</div>
										<div class="bank">7.1k</div>

										
											<div class="rnd-sq " title="11300">
												$$											</div>
										
											<div class="rnd-sq mod-win mod-ct" title="23050">
												$$$											</div>
										
										<div class="bank">
											18.9k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											17										</div>
										<div class="bank">1.8k</div>

										
											<div class="rnd-sq mod-win mod-t" title="22350">
												$$$											</div>
										
											<div class="rnd-sq " title="25100">
												$$$											</div>
										
										<div class="bank">
											27.1k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											18										</div>
										<div class="bank">4.7k</div>

										
											<div class="rnd-sq mod-win mod-t" title="21800">
												$$$											</div>
										
											<div class="rnd-sq " title="25400">
												$$$											</div>
										
										<div class="bank">
											12.1k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											19										</div>
										<div class="bank">6.0k</div>

										
											<div class="rnd-sq mod-win mod-t" title="22800">
												$$$											</div>
										
											<div class="rnd-sq " title="22950">
												$$$											</div>
										
										<div class="bank">
											4.0k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											20										</div>
										<div class="bank">8.1k</div>

										
											<div class="rnd-sq mod-win mod-t" title="23550">
												$$$											</div>
										
											<div class="rnd-sq " title="10650">
												$$											</div>
										
										<div class="bank">
											8.9k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											21										</div>
										<div class="bank">9.8k</div>

										
											<div class="rnd-sq " title="23550">
												$$$											</div>
										
											<div class="rnd-sq mod-win mod-ct" title="23550">
												$$$											</div>
										
										<div class="bank">
											1.2k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											22										</div>
										<div class="bank">3.1k</div>

										
											<div class="rnd-sq mod-win mod-t" title="19800">
												$$											</div>
										
											<div class="rnd-sq " title="23850">
												$$$											</div>
										
										<div class="bank">
											13.2k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											23										</div>
										<div class="bank">11.2k</div>

										
											<div class="rnd-sq mod-win mod-t" title="23800">
												$$$											</div>
										
											<div class="rnd-sq " title="24350">
												$$$											</div>
										
										<div class="bank">
											6.2k										</div>
										
									</td>
																																<td>

										<div class="ge-text-light round-num">
											24										</div>
										<div class="bank">8.8k</div>

										
											<div class="rnd-sq mod-win mod-t" title="24600">
												$$$											</div>
										
											<div class="rnd-sq " title="21600">
												$$$											</div>
										
										<div class="bank">
											2.6k										</div>
										
									</td>
																					</tr>
															</tbody></table>
			</div>
'''

result = scrape_econ_table(html_string,123,1412)
print(result)