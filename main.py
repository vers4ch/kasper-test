from queries import get_vulnerabilities_by_product, get_top_5_vendors_with_sui_tag


product_name = "Microsoft Windows 10" #Продукт

vulnerabilities = get_vulnerabilities_by_product(product_name)
print(f"\n\nVulnerabilities for '{product_name}':\n")
for vuln in vulnerabilities:
    print(f"Vulnerability: {vuln[0]}, Link: {vuln[1]}")




tag = 'SUI' #По какому тэгу ищем
count = 10 #Топ

top_vendors = get_top_5_vendors_with_sui_tag(tag, count)
print(f"\n\nTop {count} vendors with {tag} tag:")
for vendor in top_vendors:
    print(f"{vendor[0]} = {vendor[1]}")
