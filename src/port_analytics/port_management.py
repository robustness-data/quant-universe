"""
This module contains the classes and functions for managing a portfolio of securities.
"""
import datetime
import pandas as pd


class TaxLot:
    def __init__(self, security_id, purchase_date, cost_basis, quantity):
        self.security_id = security_id
        self.purchase_date = pd.to_datetime(purchase_date)
        self.cost_basis = cost_basis
        self.quantity = quantity
        self.age_as_of_today = (datetime.datetime.today() - self.purchase_date).days
        self.realized_gain_loss = 0
        self.total_cost = self.calculate_total_cost()

    def calculate_total_cost(self):
        return self.cost_basis * self.quantity
    
    def calculate_market_value(self, current_price):
        return current_price * self.quantity
    
    def calculate_gain_loss(self, current_price):
        return (current_price - self.cost_basis) * self.quantity

    def calculate_return(self, current_price):
        return (current_price - self.cost_basis) / self.cost_basis

    def update_quantity(self, new_quantity):
        if new_quantity < 0:
            raise ValueError("Tax lot quantity cannot be negative")
        self.quantity = new_quantity

    def realize_gain_loss(self, sale_price, sale_quantity):
        realized_gain_loss = (sale_price - self.cost_basis) * sale_quantity
        self.update_quantity(self.quantity - sale_quantity)
        self.realized_gain_loss += realized_gain_loss


class SecurityPosition:
    def __init__(self, security_id):
        self.security_id = security_id
        self.tax_lots = []
        self.current_price = 0

    def add_tax_lot(self, tax_lot):
        self.tax_lots.append(tax_lot)

    def update_current_price(self, new_price):
        self.current_price = new_price

    def calculate_position_value(self):
        return sum(tax_lot.quantity * self.current_price for tax_lot in self.tax_lots)

    def calculate_position_gain_loss(self):
        return sum(tax_lot.calculate_gain_loss(self.current_price) for tax_lot in self.tax_lots)


class Transaction:
    def __init__(self, transaction_type, security_id, transaction_date, quantity, price_per_unit):
        self.transaction_type = transaction_type
        self.security_id = security_id
        self.transaction_date = transaction_date
        self.quantity = quantity
        self.price_per_unit = price_per_unit
        self.realized_gain_loss = None

    def calculate_realized_gain_loss(self, cost_basis):
        if self.transaction_type == "Liquidation":
            self.realized_gain_loss = (self.price_per_unit - cost_basis) * self.quantity
            return self.realized_gain_loss
        else:
            return 0


class TransactionHistory:
    def __init__(self):
        self.transactions = []

    def record_transaction(self, transaction):
        self.transactions.append(transaction)

    def calculate_realized_gains_losses(self):
        total_realized_gain_loss = 0
        for transaction in self.transactions:
            if transaction.transaction_type == "Liquidation":
                # Assuming we have the cost basis available, which in a real scenario would be fetched from the tax lots
                cost_basis = 100  # Placeholder cost basis
                total_realized_gain_loss += transaction.calculate_realized_gain_loss(cost_basis)
        return total_realized_gain_loss


class Portfolio:
    def __init__(self):
        self.positions = {}
        self.transaction_history = TransactionHistory()

    def add_update_position(self, security_id, tax_lot):
        if security_id not in self.positions:
            self.positions[security_id] = SecurityPosition(security_id)
        self.positions[security_id].add_tax_lot(tax_lot)

    def record_transaction(self, transaction):
        self.transaction_history.record_transaction(transaction)
        if transaction.transaction_type == "Acquisition":
            tax_lot = TaxLot(transaction.security_id, transaction.transaction_date, transaction.price_per_unit, transaction.quantity)
            self.add_update_position(transaction.security_id, tax_lot)
        elif transaction.transaction_type == "Liquidation":
            self.handle_liquidation(transaction)

    def handle_liquidation(self, transaction):
        if transaction.security_id not in self.positions:
            return  # Security not in portfolio

        position = self.positions[transaction.security_id]
        remaining_quantity = transaction.quantity

        for tax_lot in position.tax_lots:
            if remaining_quantity <= 0:
                break

            if tax_lot.quantity > 0:
                sale_quantity = min(tax_lot.quantity, remaining_quantity)
                realized_gain_loss = tax_lot.realize_gain_loss(transaction.price_per_unit, sale_quantity)
                transaction.realized_gain_loss = (transaction.realized_gain_loss or 0) + realized_gain_loss
                remaining_quantity -= sale_quantity

    def calculate_portfolio_value(self):
        return sum(position.calculate_position_value() for position in self.positions.values())

    def calculate_portfolio_gain_loss(self):
        return sum(position.calculate_position_gain_loss() for position in self.positions.values())

    def calculate_portfolio_realized_gain_loss(self):
        return self.transaction_history.calculate_realized_gains_losses()

    def calculate_portfolio_unrealized_gain_loss(self):
        return self.calculate_portfolio_gain_loss() - self.calculate_portfolio_realized_gain_loss()

    def calculate_portfolio_return(self, current_prices=None, period_start: datetime = None) -> float:
        """
        Calculates the return for the entire portfolio over a specified period.
        """
        if period_start is None:
            return self.calculate_portfolio_gain_loss() / self.calculate_portfolio_value()

        total_return = 0.0
        for security_id, position in self.positions.items():
            if security_id in current_prices:
                total_return += self.return_calculator.calculate_return_for_security(security_id,
                                                                                     current_prices[security_id],
                                                                                     period_start)
        return total_return