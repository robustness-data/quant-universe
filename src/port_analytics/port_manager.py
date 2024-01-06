from datetime import datetime
from typing import List, Dict
import unittest


class TaxLot:
    def __init__(self, security_id: str, purchase_date: datetime, cost_basis: float, quantity: float):
        self.security_id = security_id
        self.purchase_date = purchase_date
        self.cost_basis = cost_basis
        self.quantity = quantity


class SecurityPosition:
    def __init__(self, security_id: str):
        self.security_id = security_id
        self.tax_lots = []

    def add_tax_lot(self, tax_lot: TaxLot):
        if tax_lot.security_id != self.security_id:
            raise ValueError("Tax lot security ID does not match SecurityPosition ID.")
        self.tax_lots.append(tax_lot)

    def calculate_market_value(self, current_price: float) -> float:
        return sum(tax_lot.quantity * current_price for tax_lot in self.tax_lots)

    def calculate_gains_losses(self, current_price: float) -> List[float]:
        return [(current_price - tax_lot.cost_basis) * tax_lot.quantity for tax_lot in self.tax_lots]


class Transaction:
    def __init__(self, security_id: str, transaction_date: datetime, quantity: float, price: float, transaction_type: str):
        self.security_id = security_id
        self.transaction_date = transaction_date
        self.quantity = quantity
        self.price = price
        self.transaction_type = transaction_type  # 'buy' or 'sell'
        self.realized_gain_loss = None

    def calculate_realized_gain_loss(self, cost_basis: float):
        if self.transaction_type == 'sell':
            self.realized_gain_loss = (self.price - cost_basis) * self.quantity


class TransactionHistory:
    def __init__(self):
        self.transactions = []

    def add_transaction(self, transaction: Transaction):
        self.transactions.append(transaction)

    def get_transactions_for_security(self, security_id: str) -> List[Transaction]:
        return [t for t in self.transactions if t.security_id == security_id]


class ReturnCalculator:
    def __init__(self, transaction_history: TransactionHistory):
        self.transaction_history = transaction_history

    def calculate_return_for_security(self, security_id: str, current_price: float, period_start: datetime) -> float:
        transactions = self.transaction_history.get_transactions_for_security(security_id)
        invested_amount = sum(t.price * t.quantity for t in transactions
                              if t.transaction_date >= period_start and t.transaction_type == 'buy')
        current_value = sum(t.quantity * current_price for t in transactions if t.transaction_date >= period_start)
        return (current_value - invested_amount) / invested_amount if invested_amount != 0 else 0


class Portfolio:
    def __init__(self):
        self.security_positions = {}
        self.transaction_history = TransactionHistory()
        self.return_calculator = ReturnCalculator(self.transaction_history)

    def _sell_from_tax_lot(self, tax_lot: TaxLot, quantity_to_sell: float, sell_price: float) -> float:
        """
        Handles the selling of securities from a specific tax lot.
        Returns the amount of the security sold and the realized gain or loss.
        """
        sell_quantity = min(tax_lot.quantity, quantity_to_sell)
        tax_lot.quantity -= sell_quantity
        realized_gain_loss = (sell_price - tax_lot.cost_basis) * sell_quantity
        return sell_quantity, realized_gain_loss

    def add_transaction(self, transaction: Transaction):
        """
        Adds a transaction and updates the corresponding SecurityPosition.
        """
        self.transaction_history.add_transaction(transaction)
        if transaction.security_id not in self.security_positions:
            self.security_positions[transaction.security_id] = SecurityPosition(transaction.security_id)

        if transaction.transaction_type == 'buy':
            tax_lot = TaxLot(transaction.security_id, transaction.transaction_date,
                             transaction.price, transaction.quantity)
            self.security_positions[transaction.security_id].add_tax_lot(tax_lot)

        elif transaction.transaction_type == 'sell':
            quantity_to_sell = transaction.quantity
            total_realized_gain_loss = 0.0

            for tax_lot in self.security_positions[transaction.security_id].tax_lots:
                if quantity_to_sell <= 0:
                    break
                sold_quantity, realized_gain_loss = self._sell_from_tax_lot(tax_lot, quantity_to_sell, transaction.price)
                quantity_to_sell -= sold_quantity
                total_realized_gain_loss += realized_gain_loss

            transaction.calculate_realized_gain_loss(total_realized_gain_loss / transaction.quantity)

    def calculate_portfolio_return(self, current_prices: Dict[str, float], period_start: datetime) -> float:
        """
        Calculates the return for the entire portfolio over a specified period.
        """
        total_return = 0.0
        for security_id, position in self.security_positions.items():
            if security_id in current_prices:
                total_return += self.return_calculator.calculate_return_for_security(security_id,
                                                                                     current_prices[security_id],
                                                                                     period_start)
        return total_return

    def get_portfolio_summary(self, current_prices: Dict[str, float]) -> Dict[str, float]:
        """
        Generates a summary of the portfolio, including market value, unrealized and realized gains/losses.
        """
        summary = {"total_market_value": 0.0, "total_unrealized_gain_loss": 0.0, "total_realized_gain_loss": 0.0}
        for security_id, position in self.security_positions.items():
            if security_id in current_prices:
                market_value = position.calculate_market_value(current_prices[security_id])
                unrealized_gain_loss = sum(position.calculate_gains_losses(current_prices[security_id]))
                summary["total_market_value"] += market_value
                summary["total_unrealized_gain_loss"] += unrealized_gain_loss

        # Calculating realized gain/loss
        for transaction in self.transaction_history.transactions:
            if transaction.transaction_type == 'sell':
                summary["total_realized_gain_loss"] += transaction.realized_gain_loss

        return summary


class TestPortfolioManagement(unittest.TestCase):

    def test_buy_transaction(self):
        portfolio = Portfolio()
        portfolio.add_transaction(Transaction("AAPL", datetime(2022, 1, 10), 10, 150.0, "buy"))
        self.assertEqual(len(portfolio.security_positions["AAPL"].tax_lots), 1)
        self.assertEqual(portfolio.security_positions["AAPL"].tax_lots[0].quantity, 10)

    def test_sell_transaction(self):
        portfolio = Portfolio()
        portfolio.add_transaction(Transaction("AAPL", datetime(2022, 1, 10), 10, 150.0, "buy"))
        portfolio.add_transaction(Transaction("AAPL", datetime(2023, 1, 10), 5, 170.0, "sell"))
        self.assertEqual(portfolio.security_positions["AAPL"].tax_lots[0].quantity, 5)

    def test_portfolio_return(self):
        portfolio = Portfolio()
        portfolio.add_transaction(Transaction("AAPL", datetime(2022, 1, 10), 10, 150.0, "buy"))
        current_prices = {"AAPL": 170.0}
        portfolio_return = portfolio.calculate_portfolio_return(current_prices, datetime(2022, 1, 1))
        self.assertAlmostEqual(portfolio_return, 0.7, places=2)

    def test_portfolio_summary(self):
        portfolio = Portfolio()
        portfolio.add_transaction(Transaction("AAPL", datetime(2022, 1, 10), 10, 150.0, "buy"))
        portfolio.add_transaction(Transaction("AAPL", datetime(2023, 1, 10), 5, 170.0, "sell"))
        current_prices = {"AAPL": 170.0}
        summary = portfolio.get_portfolio_summary(current_prices)
        self.assertEqual(summary["total_market_value"], 850.0)
        self.assertEqual(summary["total_realized_gain_loss"], 750.0)
        self.assertEqual(summary["total_unrealized_gain_loss"], 100.0)


if __name__ == '__main__':
    unittest.main()
